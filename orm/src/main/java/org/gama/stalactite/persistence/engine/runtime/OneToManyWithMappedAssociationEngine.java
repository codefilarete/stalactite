package org.gama.stalactite.persistence.engine.runtime;

import java.lang.reflect.Method;
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
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.AssociationTable;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.builder.CascadeManyList;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.diff.AbstractDiff;
import org.gama.stalactite.persistence.id.diff.IndexedDiff;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.collectToList;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	/** Empty setter for applying source entity to target entity (reverse side). Available only when association is mapped without intermediary table */
	static final SerializableBiConsumer NOOP_REVERSE_SETTER = (o, i) -> {
		/* Having a reverse setter in one to many relation with intermediary table isn't possible (cascadeMany.getReverseSetter() is null)
		 * because as soon as "mappedBy" is used (which fills reverseSetter), an intermediary table is not possible
		 */
	};
	
	/**
	 * Context for indexed mapped List. Will keep bean index (during insert, update and select) between "unrelated" methods/phases :
	 * indexes must be computed then applied into SQL order (and vice-versa for select), but this particular feature crosses over layers (entities
	 * and SQL) which is not implemented. In such circumstances, ThreadLocal comes to the rescue.
	 * Could be static, but would lack the O typing, which leads to some generics errors, so left non static (acceptable small overhead)
	 */
	private final ThreadLocal<Map<O, Integer>> updatableListIndex = new ThreadLocal<>();
	
	/** Setter for applying source entity to reverse side (target entity). Available only when association is mapped without intermediary table */
	private final BiConsumer<O, I> reverseSetter;
	
	public OneToManyWithMappedAssociationEngine(BiConsumer<O, I> reverseSetter) {
		this.reverseSetter = reverseSetter;
	}
	
	public <T extends Table<T>> void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
													  JoinedTablesPersister<I, J, T> joinedTablesPersister,
													  Persister<O, J, ?> targetPersister,
													  Column leftColumn,
													  Function<I, C> collectionGetter,
													  Column rightColumn,    // can be either the foreign key, or primary key, on the target table
													  PersisterListener<I, J> persisterListener) {
		
		BeanRelationFixer relationFixer;
		IMutator<I, C> collectionSetter = Accessors.<I, C>of(cascadeMany.getMember()).getMutator();
		// configuring select for fetching relation
		SerializableBiConsumer<O, I> reverseMember = Objects.preventNull(cascadeMany.getReverseSetter(), NOOP_REVERSE_SETTER);
		
		if (cascadeMany instanceof CascadeManyList) {
			// we add an index capturer
			CascadeManyList<I, O, J, List<O>> refinedCascadeMany = (CascadeManyList<I, O, J, List<O>>) cascadeMany;
			Column indexingColumn = refinedCascadeMany.getIndexingColumn();
			relationFixer = addIndexSelection(
					joinedTablesPersister, targetPersister,
					refinedCascadeMany.getCollectionTargetClass(),
					(Function<I, List<O>>) collectionGetter, persisterListener,
					(IMutator<I, List<O>>) collectionSetter, reverseMember, indexingColumn);
			joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
					targetPersister,
					relationFixer,
					leftColumn,
					rightColumn,
					true);
		} else {
			relationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
					cascadeMany.getCollectionTargetClass(), reverseMember);
			
			joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
					targetPersister,
					relationFixer,
					leftColumn,
					rightColumn,
					true);
		}
	}
	
	private <T extends Table<T>, L extends List<O>> BeanRelationFixer addIndexSelection(JoinedTablesPersister<I, J, T> joinedTablesPersister,
																	 Persister<O, J, ?> targetPersister,
																	 Class<L> collectionTargetClass,
																	 Function<I, L> collectionGetter,
																	 PersisterListener<I, J> persisterListener,
																	 IMutator<I, L> collectionSetter,
																	 SerializableBiConsumer<O, I> reverseMember,
																	 Column indexingColumn) {
		targetPersister.getSelectExecutor().getMappingStrategy().addSilentColumnSelecter(indexingColumn);
		// Implementation note: 2 possiblities
		// - keep object indexes and put sorted beans in a temporary List, then add them all to the target List
		// - keep object indexes and sort the target List throught a comparator of indexes
		// The latter is used because target List is already filled by the relationFixer
		// If we use the former we must change the relation fixer and keep a temporary List. Seems little bit more complex.
		// May be changed if any performance issue is noticed
		persisterListener.addSelectListener(new SelectListener<I, J>() {
			@Override
			public void beforeSelect(Iterable<J> ids) {
				updatableListIndex.set(new HashMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends I> result) {
				try {
					// reordering List element according to read indexes during the transforming phase (see below)
					result.forEach(i -> {
						List<O> apply = collectionGetter.apply(i);
						apply.sort(Comparator.comparingInt(o -> updatableListIndex.get().get(o)));
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<J> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				updatableListIndex.remove();
			}
		});
		// Adding a transformer listener to keep track of the index column read from ResultSet/Row
		// We place it into a ThreadLocal, then the select listener will use it to reorder loaded beans
		targetPersister.getMappingStrategy().getRowTransformer().addTransformerListener((bean, row) -> {
			Map<O, Integer> indexPerBean = updatableListIndex.get();
			// Indexing column is not defined in targetPersister.getMappingStrategy().getRowTransformer() but is present in row
			// because it was read from ResultSet
			// So we get its alias from the object that managed id, and we simply read it from the row (but not from RowTransformer)
			Map<Column, String> aliases = joinedTablesPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getAliases();
			indexPerBean.put(bean, (int) row.get(aliases.get(indexingColumn)));
		});
		return BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				collectionTargetClass, reverseMember);
	}
	
	public void addInsertCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Persister<O, J, ?> targetPersister,
								 Function<I, C> collectionGetter,
								 PersisterListener<I, J> persisterListener) {
		// For a List and a given manner to get its owner (so we can deduce index value), we configure persistence to keep index value in database
		if (cascadeMany instanceof CascadeManyList && cascadeMany.getReverseGetter() != null) {
			addIndexInsertion((CascadeManyList<I, O, J, ? extends List<O>>) cascadeMany, targetPersister);
		}
		persisterListener.addInsertListener(new TargetInstancesInsertCascader<>(targetPersister, collectionGetter));
	}
	
	/**
	 * Adds a "listener" that will amend insertion of the index column filled with its value
	 * @param cascadeMany
	 * @param targetPersister
	 */
	private void addIndexInsertion(CascadeManyList<I, O, J, ? extends List<O>> cascadeMany, Persister<O, J, ?> targetPersister) {
		// we declare the indexing column as a silent one, then AfterInsertCollectionCascader will insert it
		targetPersister.getInsertExecutor().getMappingStrategy().addSilentColumnInserter(cascadeMany.getIndexingColumn(),
				(Function<O, Object>) target -> {
					I source = cascadeMany.getReverseGetter().apply(target);
					if (source == null) {
						MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
						Method method = methodReferenceCapturer.findMethod(cascadeMany.getReverseGetter());
						throw new IllegalStateException("Can't get index : " + target + " is not associated with a " + Reflections.toString(method.getReturnType()) + " : "
								+ Reflections.toString(method) + " returned null");
					}
					List<O> collection = cascadeMany.getTargetProvider().apply(source);
					return collection.indexOf(target);
				});
	}
	
	public void addUpdateCascade(CascadeMany<I, O, J, C> cascadeMany,
								  Persister<O, J, ?> targetPersister,
								  Function<I, C> collectionGetter,
								  PersisterListener<I, J> persisterListener,
								  boolean shouldDeleteRemoved) {
		BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener;
		if (cascadeMany instanceof CascadeManyList) {
			
			targetPersister.getMappingStrategy().addSilentColumnUpdater(((CascadeManyList) cascadeMany).getIndexingColumn(),
					// Thread safe by updatableListIndex access
					(Function<O, Object>) o -> Nullable.nullable(updatableListIndex.get()).apply(m -> m.get(o)).get());
			updateListener = new CollectionUpdater<I, O, C>(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved) {
				
				protected Set<? extends AbstractDiff> diff(Collection<O> modified, Collection<O> unmodified) {
					return differ.diffList((List) unmodified, (List) modified);
				}
				
				@Override
				protected UpdateContext newUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
					return new IndexedMappedAssociationUpdateContext(updatePayload);
				}
				
				@Override
				protected void onHeldTarget(UpdateContext updateContext, AbstractDiff diff) {
					super.onHeldTarget(updateContext, diff);
					Set<Integer> minus = Iterables.minus(((IndexedDiff) diff).getReplacerIndexes(), ((IndexedDiff) diff).getSourceIndexes());
					Integer index = Iterables.first(minus);
					if (index != null ) {
						((IndexedMappedAssociationUpdateContext) updateContext).getIndexUpdates().put((O) diff.getReplacingInstance(), index);
					}
				}
				
				@Override
				protected void updateTargets(UpdateContext updateContext, boolean allColumnsStatement) {
					// we ask for entities update, code seems weird because we pass a Duo of same instance which should update nothing because
					// entities are stricly equal, but in fact this update() invokation triggers the "silent column" update, which is the index column
					ThreadLocals.doWithThreadLocal(updatableListIndex, ((IndexedMappedAssociationUpdateContext) updateContext)::getIndexUpdates, (Runnable) () -> {
						List<Duo<? extends O, ? extends O>> entities = collectToList(updatableListIndex.get().keySet(), o -> new Duo<>(o, o));
						targetPersister.update(entities, false);
					});
				}
				
				class IndexedMappedAssociationUpdateContext extends UpdateContext {
					
					/** New indexes per entity */
					private final Map<O, Integer> indexUpdates = new HashMap<>();
					
					public IndexedMappedAssociationUpdateContext(UpdatePayload<? extends I, ?> updatePayload) {
						super(updatePayload);
					}
					
					public Map<O, Integer> getIndexUpdates() {
						return indexUpdates;
					}
				}
			};
		} else /* any other type of Collection except List */ {
			updateListener = new CollectionUpdater<>(collectionGetter, targetPersister, reverseSetter, shouldDeleteRemoved);
		}
		persisterListener.addUpdateListener(new TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public <T extends Table<T>> void addDeleteCascade(CascadeMany<I, O, J, C> cascadeMany,
													  JoinedTablesPersister<I, J, T> joinedTablesPersister,
													  Persister<O, J, ?> targetPersister,
													  Function<I, C> collectionGetter,
													  PersisterListener<I, J> persisterListener,
													  boolean deleteTargetEntities,
													  Dialect dialect,
													  Column<? extends AssociationTable, Object> pointerToLeftColumn) {
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, O>(targetPersister) {
				
				@Override
				protected void postTargetDelete(Iterable<O> entities) {
					// no post treatment to do
				}
				
				@Override
				protected Collection<O> getTargets(I i) {
					Collection<O> targets = collectionGetter.apply(i);
					// We only delete persisted instances (for logic and to prevent from non matching row count exception)
					return stream(targets)
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
				protected Collection<O> getTargets(I i) {
					Collection<O> targets = collectionGetter.apply(i);
					// We only delete persisted instances (for logic and to prevent from non matching row count exception)
					return stream(targets)
							.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
							.collect(Collectors.toList());
				}
			});
		} else // entity shouldn't be deleted, so we may have to update it
			if (reverseSetter != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				
				persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, O>(targetPersister) {
					
					@Override
					protected void postTargetDelete(Iterable<O> entities) {
						
					}
					
					@Override
					public void beforeDelete(Iterable<I> entities) {
						List<O> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> reverseSetter.accept(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<O> getTargets(I i) {
						Collection<O> targets = collectionGetter.apply(i);
						// We only delete persisted instances (for logic and to prevent from non matching row count exception)
						return stream(targets)
								.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
								.collect(Collectors.toList());
					}
				});
			}
	}

	static class TargetInstancesInsertCascader<I extends Identified, O extends Identified, J extends Identifier> extends AfterInsertCollectionCascader<I, O> {

		private final Function<I, ? extends Collection<O>> collectionGetter;

		public TargetInstancesInsertCascader(Persister<O, J, ?> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}

		@Override
		protected void postTargetInsert(Iterable<? extends O> entities) {
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
	}
	
	static class TargetInstancesUpdateCascader<I extends Identified, O extends Identified> extends AfterUpdateCollectionCascader<I, O> {
		
		private final BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener;
		
		public TargetInstancesUpdateCascader(Persister<O, ?, ?> targetPersister, BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener) {
			super(targetPersister);
			this.updateListener = updateListener;
		}
		
		@Override
		public void afterUpdate(Iterable<UpdatePayload<? extends I, ?>> entities, boolean allColumnsStatement) {
			entities.forEach(entry -> updateListener.accept(entry, allColumnsStatement));
		}
		
		@Override
		protected void postTargetUpdate(Iterable<UpdatePayload<? extends O, ?>> entities) {
			// Nothing to do
		}
		
		@Override
		protected Collection<Duo<O, O>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
			throw new NotYetSupportedOperationException();
		}
	}
	
}
