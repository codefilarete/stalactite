package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.CascadeMany;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.ISelectListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class CascadeManyConfigurer<I extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
	
	/** {@link OneToManyOptions#mappedBy(SerializableBiConsumer)} method signature (for printing purpose) to help find usage by avoiding hard "mappedBy" String */
	private static final String MAPPED_BY_SIGNATURE;
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue. {@link CascadeManyConfigurer} leads its management.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<O, Integer>> updatableListIndex = new ThreadLocal<>();
	
	static {
		Method mappedByMethod = new MethodReferenceCapturer()
				.findMethod((SerializableBiConsumer<OneToManyOptions, SerializableBiConsumer>) OneToManyOptions::mappedBy);
		MAPPED_BY_SIGNATURE = mappedByMethod.getDeclaringClass().getSimpleName() + "." + mappedByMethod.getName();
	}
	
	private IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	void setDiffer(IdentifiedCollectionDiffer differ) {
		this.differ = differ;
	}
	
	public <T extends Table> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
												JoinedTablesPersister<I, J, T> joinedTablesPersister,
												ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Persister<O, J, Table> targetPersister = cascadeMany.getPersister();
		
		// adding persistence flag setters on both side
		joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<I>) SetPersistedFlagAfterInsertListener.INSTANCE);
		targetPersister.getPersisterListener().addInsertListener((IInsertListener<O>) SetPersistedFlagAfterInsertListener.INSTANCE);
		
		// finding joined columns: left one is primary key. Right one is given by the target strategy through the property accessor
		Column leftColumn = joinedTablesPersister.getTargetTable().getPrimaryKey();
		Function<I, C> collectionGetter = cascadeMany.getTargetProvider();
		if (cascadeMany.getReverseSetter() == null && cascadeMany.getReverseGetter() == null && cascadeMany.getReverseColumn() == null) {
			throw new NotYetSupportedOperationException("Collection mapping without reverse property is not (yet) supported,"
					+ " please use \"" + MAPPED_BY_SIGNATURE + "\" option do declare one for " 
					+ Reflections.toString(cascadeMany.getMember()));
		}
		
		Column rightColumn = cascadeMany.getReverseColumn();
		if (rightColumn == null) {
			// Here reverse side is surely defined by method reference (because of assertion some lines upper), we look for the matching column
			MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
			Method reverseMember;
			if (cascadeMany.getReverseSetter() != null) {
				reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseSetter());
			} else {
				reverseMember = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
			}
			rightColumn = targetPersister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(Accessors.of(reverseMember));
			if (rightColumn == null) {
				throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
						+ Reflections.toString(joinedTablesPersister.getMappingStrategy().getClassToPersist())
						+ " to persister of " + cascadeMany.getPersister().getMappingStrategy().getClassToPersist().getName());
			}
		}
		
		// adding foreign key constraint
		rightColumn.getTable().addForeignKey(foreignKeyNamingStrategy.giveName(rightColumn, leftColumn), rightColumn, leftColumn);
		
		// managing cascades
		PersisterListener<I, J> persisterListener = joinedTablesPersister.getPersisterListener();
		for (CascadeType cascadeType : cascadeMany.getCascadeTypes()) {
			switch (cascadeType) {
				case INSERT:
					if (cascadeMany.getIndexingColumn() != null) {
						if (!List.class.isAssignableFrom(cascadeMany.getMember().getReturnType())) {
							throw new UnsupportedOperationException("Indexing column is only available on List, found " + Reflections.toString(cascadeMany.getMember().getReturnType()));
						}
						// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
						targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(cascadeMany.getIndexingColumn(),
								(Function<O, Object>) target -> {
									I source = cascadeMany.getReverseGetter().apply(target);
									C collection = cascadeMany.getTargetProvider().apply(source);
									return ((List) collection).indexOf(target);
								});
					}
					persisterListener.addInsertListener(new AfterInsertCollectionCascader<I, O>(targetPersister) {
						
						@Override
						protected void postTargetInsert(Iterable<O> entities) {
							// Nothing to do. Identified#isPersisted flag should be fixed by target persister
						}
						
						@Override
						protected Collection<O> getTargets(I o) {
							Collection<O> targets = collectionGetter.apply(o);
							// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.NON_PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
				case UPDATE:
					if (cascadeMany.getIndexingColumn() != null && !List.class.isAssignableFrom(cascadeMany.getMember().getReturnType())) {
						throw new UnsupportedOperationException("Indexing column is only available on List, found "
								+ Reflections.toString(cascadeMany.getMember().getReturnType()));
					}
					BiConsumer<Duo<I, I>, Boolean> updateListener;
					if (List.class.isAssignableFrom(cascadeMany.getMember().getReturnType())) {
						updateListener = (entry, allColumnsStatement) -> {
							C modified = collectionGetter.apply(entry.getLeft());
							C unmodified = collectionGetter.apply(entry.getRight());
							
							// In order to have batch update of the index column (better performance) we compute the whole indexes
							// Then those indexes will be given to the update cascader.
							// But this can only be done through a ThreadLocal (for now) because there's no way to give them directly
							// Hence we need to be carefull of Thread safety (cleaning context and collision)
							
							Set<Diff> diffSet = differ.diffList((List) unmodified, (List) modified);
							// a List to keep SQL orders, for better debug, easier understanding of logs
							List<O> toBeInserted = new ArrayList<>();
							List<O> toBeDeleted = new ArrayList<>();
							Map<O, Integer> newIndexes = new HashMap<>();
							for (Diff diff : diffSet) {
								switch (diff.getState()) {
									case ADDED:
										toBeInserted.add((O) diff.getReplacingInstance());
										break;
									case HELD:
										O replacingInstance = (O) diff.getReplacingInstance();
										I source = cascadeMany.getReverseGetter().apply(replacingInstance);
										C collection = cascadeMany.getTargetProvider().apply(source);
										int index = ((List) collection).indexOf(diff.getReplacingInstance());
										newIndexes.put(replacingInstance, index);
										targetPersister.getMappingStrategy().addSilentColumnUpdater(cascadeMany.getIndexingColumn(),
												// Thread safe by updatableListIndex access
												(Function<O, Object>) o -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(o)).get());
										break;
									case REMOVED:
										if (cascadeMany.shouldDeleteRemoved()) {
											toBeDeleted.add((O) diff.getSourceInstance());
										}
										break;
								}
							}
							// we batch index update
							ThreadLocals.doWithThreadLocal(updatableListIndex, () -> newIndexes, (Runnable) () -> {
								List<Duo<O, O>> collect = Iterables.collectToList(updatableListIndex.get().keySet(), o -> new Duo<>(o, o));
								targetPersister.update(collect, false);
							});
							// we batch added and deleted objects
							targetPersister.insert(toBeInserted);
							targetPersister.delete(toBeDeleted);
						};
					} else /* any other type of Collection except List */ {
						updateListener = (entry, allColumnsStatement) -> {
							C modified = collectionGetter.apply(entry.getLeft());
							C unmodified = collectionGetter.apply(entry.getRight());
							Set<Diff> diffSet = differ.diffSet((Set) unmodified, (Set) modified);
							for (Diff diff : diffSet) {
								switch (diff.getState()) {
									case ADDED:
										targetPersister.insert((O) diff.getReplacingInstance());
										break;
									case HELD:
										// NB: update will only be done if necessary by target persister
										targetPersister.update((O) diff.getReplacingInstance(), (O) diff.getSourceInstance(), allColumnsStatement);
										break;
									case REMOVED:
										if (cascadeMany.shouldDeleteRemoved()) {
											targetPersister.delete((O) diff.getSourceInstance());
										}
										break;
								}
							}
						};
					}
					persisterListener.addUpdateListener(new AfterUpdateCollectionCascader<I, O>(targetPersister) {
						@Override
						public void afterUpdate(Iterable<Duo<I, I>> entities, boolean allColumnsStatement) {
							entities.forEach(entry -> updateListener.accept(entry, allColumnsStatement));
						}
						
						@Override
						protected void postTargetUpdate(Iterable<Duo<O, O>> entities) {
							// Nothing to do
						}
						
						@Override
						protected Collection<Duo<O, O>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
							throw new NotYetSupportedOperationException();
						}
					});
				break;
				case DELETE:
					persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, O>(targetPersister) {
						
						@Override
						protected void postTargetDelete(Iterable<O> entities) {
							// no post treatment to do
						}
						
						@Override
						protected Collection<O> getTargets(I o) {
							Collection<O> targets = collectionGetter.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
					persisterListener.addDeleteByIdListener(new BeforeDeleteByIdCollectionCascader<I, O>(targetPersister) {
						@Override
						protected void postTargetDelete(Iterable<O> entities) {
							// no post treatment to do
						}
						
						@Override
						protected Collection<O> getTargets(I o) {
							Collection<O> targets = collectionGetter.apply(o);
							// We only delete persisted instances (for logic and to prevent from non matching row count exception)
							return Iterables.stream(targets)
									.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
									.collect(Collectors.toList());
						}
					});
					break;
				case SELECT:
					BeanRelationFixer relationFixer;
					IMutator collectionSetter = Accessors.of(cascadeMany.getMember()).getMutator();
					// configuring select for fetching relation
					SerializableBiConsumer<O, I> reverseMember = cascadeMany.getReverseSetter();
					if (reverseMember == null) {
						reverseMember = (o, i) -> { /* we can't do anything, so we do ... nothing */ };
					}
					
					if (cascadeMany.getIndexingColumn() != null) {
						if (!List.class.isAssignableFrom(cascadeMany.getMember().getReturnType())) {
							throw new UnsupportedOperationException("Indexing column is only available on List, found "
									+ Reflections.toString(cascadeMany.getMember().getReturnType()));
						}
						targetPersister.getSelectExecutor().getMappingStrategy().addSilentColumnSelecter(cascadeMany.getIndexingColumn());
						// Implementation note: 2 possiblities
						// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
						// - keep object indexes and sort the target List throught a comparator of indexes
						// The latter is used because target List is already filled by the relationFixer
						// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
						// May be changed if any performance issue is noticed
						persisterListener.addSelectListener(new ISelectListener<I, J>() {
							@Override
							public void beforeSelect(Iterable<J> ids) {
								updatableListIndex.set(new HashMap<>());
							}
							
							@Override
							public void afterSelect(Iterable<I> result) {
								// reordering List element according to read indexes during the transforming phase (see below)
								result.forEach(i -> {
									List<O> apply = (List<O>) cascadeMany.getTargetProvider().apply(i);
									apply.sort(Comparator.comparingInt(o -> updatableListIndex.get().get(o)));
								});
								cleanContext();
							}
							
							@Override
							public void onError(Iterable<J> ids, RuntimeException exception) {
								cleanContext();
							}
							
							private void cleanContext() {
								updatableListIndex.remove();
							}
						});
						relationFixer = BeanRelationFixer.of((BiConsumer) collectionSetter::set, collectionGetter,
								cascadeMany.getCollectionTargetClass(), reverseMember);
						Column indexingColumn = cascadeMany.getIndexingColumn();
						// Adding a transformer listener to keep track of the index column read from ResultSet/Row
						// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
						targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
							
							Map<O, Integer> indexPerBean = updatableListIndex.get();
							Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
							String s = aliases.get(indexingColumn);
							indexPerBean.put(bean, (int) targetPersister.getMappingStrategy().getRowTransformer().getValue(row, s));
						});
					} else {
						relationFixer = BeanRelationFixer.of((BiConsumer) collectionSetter::set, collectionGetter,
								cascadeMany.getCollectionTargetClass(), reverseMember);
						
					}
					joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
							relationFixer,
							leftColumn, rightColumn, true);
					break;
			}
		}
	}
}
