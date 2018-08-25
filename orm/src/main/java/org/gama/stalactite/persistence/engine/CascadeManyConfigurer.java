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
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.SetPersistedFlagAfterInsertListener;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.ISelectListener;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.diff.Diff;
import org.gama.stalactite.persistence.id.diff.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
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
	
	private final IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
	
	public <T extends Table<T>> void appendCascade(CascadeMany<I, O, J, C> cascadeMany,
												   JoinedTablesPersister<I, J, T> joinedTablesPersister,
												   ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Persister<O, J, ?> targetPersister = cascadeMany.getPersister();
		
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
					addInsertCascade(cascadeMany, targetPersister, collectionGetter, persisterListener);
					break;
				case UPDATE:
					addUpdateCascade(cascadeMany, targetPersister, collectionGetter, persisterListener);
					break;
				case DELETE:
					addDeleteCascade(targetPersister, collectionGetter, persisterListener);
					break;
				case SELECT:
					addSelectCascade(cascadeMany, joinedTablesPersister, targetPersister, leftColumn, collectionGetter, rightColumn,
							persisterListener);
					break;
			}
		}
	}
	
	private <T extends Table<T>> void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
													  JoinedTablesPersister<I, J, T> joinedTablesPersister,
													  Persister<O, J, ?> targetPersister,
													  Column leftColumn,
													  Function<I, C> collectionGetter,
													  Column rightColumn,
													  PersisterListener<I, J> persisterListener) {
		BeanRelationFixer relationFixer;
		PropertyAccessor<I, C> propertyAccessor = Accessors.of(cascadeMany.getMember());
		IMutator<I, C> collectionSetter = propertyAccessor.getMutator();
		// configuring select for fetching relation
		SerializableBiConsumer<O, I> reverseMember = cascadeMany.getReverseSetter();
		if (reverseMember == null) {
			reverseMember = (o, i) -> { /* we can't do anything, so we do ... nothing */ };
		}
		
		if (cascadeMany instanceof CascadeManyList) {
			relationFixer = addIndexSelection((CascadeManyList<I, O, J>) cascadeMany,
					joinedTablesPersister, targetPersister, (Function<I, List<O>>) collectionGetter, persisterListener,
					(IMutator<I, List<O>>) collectionSetter, reverseMember);
		} else {
			relationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
					cascadeMany.getCollectionTargetClass(), reverseMember);
			
		}
		joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
				relationFixer,
				leftColumn, rightColumn, true);
	}
	
	private <T extends Table<T>> BeanRelationFixer addIndexSelection(CascadeManyList<I, O, J> cascadeMany,
																	JoinedTablesPersister<I, J, T> joinedTablesPersister,
																	Persister<O, J, ?> targetPersister,
																	Function<I, List<O>> collectionGetter,
																	PersisterListener<I, J> persisterListener,
																	IMutator<I, List<O>> collectionSetter,
																	SerializableBiConsumer<O, I> reverseMember) {
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
					List<O> apply = cascadeMany.getTargetProvider().apply(i);
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
		BeanRelationFixer relationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				cascadeMany.getCollectionTargetClass(), reverseMember);
		Column indexingColumn = cascadeMany.getIndexingColumn();
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
			
			Map<O, Integer> indexPerBean = updatableListIndex.get();
			// indexingColumn is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present is row
			// because it was read from ResultSet
			// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
			Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
			indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
		});
		return relationFixer;
	}
	
	private void addDeleteCascade(Persister<O, J, ?> targetPersister, Function<I, C> collectionGetter, PersisterListener<I, J> persisterListener) {
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
	}
	
	private void addUpdateCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener) {
		BiConsumer<UpdatePayload<I, ?>, Boolean> updateListener;
		if (cascadeMany instanceof CascadeManyList) {
			updateListener = addIndexUpdate((CascadeManyList<I, O, J>) cascadeMany, targetPersister, collectionGetter);
		} else /* any other type of Collection except List */ {
			updateListener = (entry, allColumnsStatement) -> {
				C modified = collectionGetter.apply(entry.getEntities().getLeft());
				C unmodified = collectionGetter.apply(entry.getEntities().getRight());
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
			public void afterUpdate(Iterable<UpdatePayload<I, ?>> entities, boolean allColumnsStatement) {
				entities.forEach(entry -> updateListener.accept(entry, allColumnsStatement));
			}
			
			@Override
			protected void postTargetUpdate(Iterable<UpdatePayload<O, ?>> entities) {
				// Nothing to do
			}
			
			@Override
			protected Collection<Duo<O, O>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
				throw new NotYetSupportedOperationException();
			}
		});
	}
	
	private BiConsumer<UpdatePayload<I, ?>, Boolean> addIndexUpdate(CascadeManyList<I, O, J> cascadeMany,
																   Persister<O, J, ?> targetPersister,
																   Function<I, C> collectionGetter) {
		BiConsumer<UpdatePayload<I, ?>, Boolean> updateListener;
		updateListener = (updatePayload, allColumnsStatement) -> {
			C modified = collectionGetter.apply(updatePayload.getEntities().getLeft());
			C unmodified = collectionGetter.apply(updatePayload.getEntities().getRight());
			
			// In order to have batch update of the index column (better performance) we compute the whole indexes
			// Then those indexes will be given to the update cascader.
			// But this can only be done through a ThreadLocal (for now) because there's no way to give them directly
			// Hence we need to be carefull of Thread safety (cleaning context and collision)
			
			Set<IndexedDiff> diffSet = differ.diffList((List) unmodified, (List) modified);
			// a List to keep SQL orders, for better debug, easier understanding of logs
			List<O> toBeInserted = new ArrayList<>();
			List<O> toBeDeleted = new ArrayList<>();
			Map<O, Integer> newIndexes = new HashMap<>();
			for (IndexedDiff diff : diffSet) {
				switch (diff.getState()) {
					case ADDED:
						// we insert only non persisted entity to prevent from a primary key conflict
						if (!diff.getReplacingInstance().getId().isPersisted()) {
							toBeInserted.add((O) diff.getReplacingInstance());
						}
						break;
					case HELD:
						Integer index = Iterables.first(Iterables.minus(diff.getReplacerIndexes(), diff.getSourceIndexes()));
						if (index != null ) {
							newIndexes.put((O) diff.getReplacingInstance(), index);
							targetPersister.getMappingStrategy().addSilentColumnUpdater(cascadeMany.getIndexingColumn(),
									// Thread safe by updatableListIndex access
									(Function<O, Object>) o -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(o)).get());
						}
						break;
					case REMOVED:
						// we delete only persisted entity to prevent from a not found record
						if (cascadeMany.shouldDeleteRemoved() && diff.getSourceInstance().getId().isPersisted()) {
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
		return updateListener;
	}
	
	public void addInsertCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener) {
		if (cascadeMany instanceof CascadeManyList) {
			addIndexInsertion((CascadeManyList<I, O, J>) cascadeMany, targetPersister);
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
	}
	
	private void addIndexInsertion(CascadeManyList<I, O, J> cascadeMany, Persister<O, J, ?> targetPersister) {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(cascadeMany.getIndexingColumn(),
				(Function<O, Object>) target -> {
					I source = cascadeMany.getReverseGetter().apply(target);
					List<O> collection = cascadeMany.getTargetProvider().apply(source);
					return collection.indexOf(target);
				});
	}
}
