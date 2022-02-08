package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.gama.reflection.AccessorDefinition;
import org.gama.reflection.ReversibleAccessor;
import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.configurer.CascadeManyConfigurer.FirstPhaseCycleLoadListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.codefilarete.tool.bean.Objects.not;
import static org.codefilarete.tool.collection.Iterables.stream;
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
	
	protected final EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
	
	protected final EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister;
	
	protected final MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor;
	
	public OneToManyWithMappedAssociationEngine(EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
												MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDescriptor,
												EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister) {
		this.targetPersister = targetPersister;
		this.manyRelationDescriptor = manyRelationDescriptor;
		this.sourcePersister = sourcePersister;
	}
	
	public MappedManyRelationDescriptor<SRC, TRGT, C> getManyRelationDescriptor() {
		return manyRelationDescriptor;
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
	
	/**
	 * Method to be invoked in case of entity cycle detected in its persistence configuration.
	 * We add a second phase load because cycle can hardly be supported by simply joining things together, in particular due to that
	 * 				
	 * @param sourcePrimaryKey left table primary key
	 * @param relationOwner right table primary key
	 * @param collectionGetter relation provider
	 * @param firstPhaseCycleLoadListener code to be invoked when reading rows
	 */
	public void addSelectCascadeIn2Phases(Column sourcePrimaryKey,
										  Column relationOwner,
										  ReversibleAccessor<SRC, C> collectionGetter,
										  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		// Join is declared on non-added tables : Person (alias = null) / Person (alias = null)
		Table relationOwnerTableClone = new Table(relationOwner.getTable().getName());
		Column relationOwnerPrimaryKey = relationOwnerTableClone.addColumn(sourcePrimaryKey.getName(), sourcePrimaryKey.getJavaType());
		Column relationOwnerClone = relationOwnerTableClone.addColumn(relationOwner.getName(), relationOwner.getJavaType());
		
		sourcePersister.getEntityJoinTree().addPassiveJoin(ROOT_STRATEGY_NAME,
				sourcePrimaryKey,
				relationOwnerClone,
				relationOwnerTableClone.getName() + "_" + AccessorDefinition.giveDefinition(collectionGetter).getName(),
				JoinType.OUTER, (Set) Arrays.asSet(relationOwnerPrimaryKey),
				(src, rowValueProvider) -> firstPhaseCycleLoadListener.onFirstPhaseRowRead(src, (TRGTID) rowValueProvider.apply(relationOwnerPrimaryKey))
				, false);
	}
	
	public static class TargetInstancesInsertCascader<I, O, J> extends AfterInsertCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<O, J> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
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
		
		public TargetInstancesUpdateCascader(EntityPersister<O, ?> targetPersister, BiConsumer<? extends Duo<? extends I, ? extends I>, Boolean> updateListener) {
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
		
		public DeleteTargetEntitiesBeforeDeleteCascader(EntityPersister<O, ?> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
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
		
		public DeleteByIdTargetEntitiesBeforeDeleteByIdCascader(EntityPersister<O, ?> targetPersister,
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
