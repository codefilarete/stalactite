package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.IdentityMap;

import static org.gama.lang.bean.Objects.not;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.ROOT_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, C extends Collection<TRGT>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	public static final BiConsumer NOOP_REVERSE_SETTER = (o, i) -> {
		/* Having a reverse setter in one to many relation with intermediary table isn't possible (cascadeMany.getReverseSetter() is null)
		 * because as soon as "mappedBy" is used (which fills reverseSetter), an intermediary table is not possible
		 */
	};
	
	protected final IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
	
	protected final IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister;
	
	protected final ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public OneToManyWithMappedAssociationEngine(IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
												ManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
												IEntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister) {
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.sourcePersister = sourcePersister;
	}
	
	public void addSelectCascade(Column sourcePrimaryKey,
								 Column relationOwner    // foreign key on target table
	) {
		// we add target subgraph joins to main persister
		targetPersister.joinAsMany(sourcePersister, sourcePrimaryKey, relationOwner, manyRelationDescriptor.getRelationFixer(),
				null, EntityJoinTree.ROOT_STRATEGY_NAME, relationOwner.isNullable());
		
		// we must trigger subgraph event on loading of our own graph, this is mainly for event that initializes things because given ids
		// are not those of their entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}

			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				Iterable collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(manyRelationDescriptor.getCollectionGetter().apply(src))
						.map(Collection::stream)
						.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}

			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	public void addInsertCascade() {
		sourcePersister.addInsertListener(
				new OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new CollectionUpdater<>(
				manyRelationDescriptor.getCollectionGetter(),
				targetPersister,
				manyRelationDescriptor.getReverseSetter(),
				shouldDeleteRemoved);
		sourcePersister.addUpdateListener(
				new OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public void addDeleteCascade(boolean deleteTargetEntities) {
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			sourcePersister.addDeleteListener(
					new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(
					new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDescriptor.getCollectionGetter()));
		} else // entity shouldn't be deleted, so we may have to update it
			if (manyRelationDescriptor.getReverseSetter() != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				sourcePersister.addDeleteListener(new BeforeDeleteCollectionCascader<SRC, TRGT>(targetPersister) {
					
					@Override
					protected void postTargetDelete(Iterable<TRGT> entities) {
						// nothing to do after deletion
					}
					
					@Override
					public void beforeDelete(Iterable<SRC> entities) {
						List<TRGT> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> manyRelationDescriptor.getReverseSetter().accept(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<TRGT> getTargets(SRC src) {
						Collection<TRGT> targets = manyRelationDescriptor.getCollectionGetter().apply(src);
						// We only delete persisted instances (for logic and to prevent from non matching row count exception)
						return stream(targets)
								.filter(not(targetPersister.getMappingStrategy()::isNew))
								.collect(Collectors.toList());
					}
				});
			}
	}
	
	private final ThreadLocal<IdentityMap<SRC, Set<TRGTID>>> currentSelectedTargetEntitiesIds = new ThreadLocal<>();
	
	/**
	 * Method to be invoked in case of entity cycle detected in its persistence configuration.
	 * We add a second phase load because cycle can hardly be supported by simply joining things together, in particular due to that
	 * Query and SQL generation don't support several instances of table and columns in them (aliases generation must be inhanced),
	 * and overall column reading will be messed up because of that (to avoid all of this we should have mapping strategy clones)
	 * 				
	 * @param sourcePrimaryKey
	 * @param relationOwner
	 * @param targetPersister
	 * @param persisterRegistry
	 */
	public void add2PhasesSelectCascade(Column sourcePrimaryKey,
										Column relationOwner,
										IEntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
										IReversibleAccessor<SRC, C> collectionGetter,
										PersisterRegistry persisterRegistry) {
		// Algorithm :
		// 1- add the association table to entity load graph
		// 2- read its right column when entity is loaded
		// 3- after selection, run a select to read target entities
		// .. but this is full of pitfall:
		// - because right column reading is made in TransformerListener and selection is made by SelectListener, data (target entities ids) must be
		//  shared : this is done through a ThreadLocal
		// - to avoid stack overflow we have to clean the TrehadLocal before selecting target (happens in direct cycle : Person.children -> Person)
		
		
		// we join on the association table and add bean association in memory
		
		// Join is declared on non-added tables : Person (alias = null) / Person (alias = null)
		Table relationOwnerTableClone = new Table(relationOwner.getTable().getName());
		Column relationOwnerPrimaryKey = relationOwnerTableClone.addColumn(sourcePrimaryKey.getName(), sourcePrimaryKey.getJavaType());
		Column relationOwnerClone = relationOwnerTableClone.addColumn(relationOwner.getName(), relationOwner.getJavaType());
		
		sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				sourcePrimaryKey,
				relationOwnerClone,
				relationOwnerTableClone.getName() + "_" + AccessorDefinition.giveDefinition(collectionGetter).getName(),
				JoinType.OUTER, (Set) Arrays.asSet(relationOwnerPrimaryKey),
				(src, rowValueProvider) -> {
					IdentityMap<SRC, Set<TRGTID>> trgtIntegerIdentityMap = currentSelectedTargetEntitiesIds.get();
					Set<TRGTID> targetIds = trgtIntegerIdentityMap.get(src);
					if (targetIds == null) {
						targetIds = new HashSet<>();
						trgtIntegerIdentityMap.put(src, targetIds);
					}
					TRGTID targetId = (TRGTID) rowValueProvider.apply(relationOwnerPrimaryKey);
					if (targetId != null) {
						targetIds.add(targetId);
					}
				}, false);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				currentSelectedTargetEntitiesIds.set(new IdentityMap<>());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				try {
					// Finding the right loader must be done dynamically because targetPersister is only a light version of the definitive one 
					// since its mapping is polymorphic
					IEntityPersister<TRGT, TRGTID> persister = persisterRegistry.getPersister(targetPersister.getClassToPersist());
					// We do one request to load every target instances for better performance 
					IdentityMap<SRC, Set<TRGTID>> srcSetIdentityMap = currentSelectedTargetEntitiesIds.get();
					Set<TRGTID> targetIds = srcSetIdentityMap.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
					// We clear context before next selection to avoid stackoverflow when cycle is direct (Person.children -> Person)
					// This has no impact on other use cases
					cleanContext();
					List<TRGT> targets = persister.select(targetIds);
					// Associating target instances to source ones
					Map<TRGTID, TRGT> targetPerId = Iterables.map(targets, targetPersister::getId);
					result.forEach(src -> {
						srcSetIdentityMap.get(src).forEach(targetId -> manyRelationDescriptor.getRelationFixer().apply(src, targetPerId.get(targetId)));
					});
				} finally {
					cleanContext();
				}
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				cleanContext();
			}
			
			private void cleanContext() {
				currentSelectedTargetEntitiesIds.remove();
			}
		});
	}
	
	public static class TargetInstancesInsertCascader<I, O, J> extends AfterInsertCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public TargetInstancesInsertCascader(IEntityPersister<O, J> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends O> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<O> getTargets(I source) {
			Collection<O> targets = collectionGetter.apply(source);
			// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
			return Iterables.stream(targets)
					.filter(getPersister()::isNew)
					.collect(Collectors.toList());
		}
	}
	
	public static class TargetInstancesUpdateCascader<I, O> extends AfterUpdateCollectionCascader<I, O> {
		
		private final BiConsumer<Duo<? extends I, ? extends I>, Boolean> updateListener;
		
		public TargetInstancesUpdateCascader(IEntityPersister<O, ?> targetPersister, BiConsumer<? extends Duo<? extends I, ? extends I>, Boolean> updateListener) {
			super(targetPersister);
			this.updateListener = (BiConsumer<Duo<? extends I, ? extends I>, Boolean>) updateListener;
		}
		
		@Override
		public void afterUpdate(Iterable<? extends Duo<? extends I, ? extends I>> entities, boolean allColumnsStatement) {
			entities.forEach(entry -> updateListener.accept(entry, allColumnsStatement));
		}
		
		@Override
		protected void postTargetUpdate(Iterable<? extends Duo<? extends O, ? extends O>> entities) {
			// Nothing to do
		}
		
		@Override
		protected Collection<Duo<O, O>> getTargets(I modifiedTrigger, I unmodifiedTrigger) {
			throw new UnsupportedOperationException();
		}
	}
	
	public static class DeleteTargetEntitiesBeforeDeleteCascader<I, O> extends BeforeDeleteCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteTargetEntitiesBeforeDeleteCascader(IEntityPersister<O, ?> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetDelete(Iterable<O> entities) {
			// no post treatment to do
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			Collection<O> targets = collectionGetter.apply(i);
			// We only delete persisted instances (for logic and to prevent from non matching row count exception)
			return stream(targets)
					.filter(not(getPersister()::isNew))
					.collect(Collectors.toList());
		}
	}
	
	static class DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<I, O> extends BeforeDeleteByIdCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteByIdTargetEntitiesBeforeDeleteByIdCascader(IEntityPersister<O, ?> targetPersister,
																Function<I, ? extends Collection<O>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetDelete(Iterable<O> entities) {
			// no post treatment to do
		}
		
		@Override
		protected Collection<O> getTargets(I i) {
			Collection<O> targets = collectionGetter.apply(i);
			// We only delete persisted instances (for logic and to prevent from non matching row count exception)
			return stream(targets)
					.filter(not(getPersister()::isNew))
					.collect(Collectors.toList());
		}
	}
}
