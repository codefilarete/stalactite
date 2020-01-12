package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.IConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.structure.Column;

import static org.gama.lang.bean.Objects.not;
import static org.gama.lang.collection.Iterables.stream;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<SRC, TRGT, ID, C extends Collection<TRGT>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	protected static final BiConsumer NOOP_REVERSE_SETTER = (o, i) -> {
		/* Having a reverse setter in one to many relation with intermediary table isn't possible (cascadeMany.getReverseSetter() is null)
		 * because as soon as "mappedBy" is used (which fills reverseSetter), an intermediary table is not possible
		 */
	};
	
	protected final IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister;
	
	protected final IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister;
	
	protected final MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition;
	
	public OneToManyWithMappedAssociationEngine(IEntityConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
												MappedManyRelationDescriptor<SRC, TRGT, C> manyRelationDefinition,
												IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister) {
		this.targetPersister = targetPersister;
		this.manyRelationDefinition = manyRelationDefinition;
		this.sourcePersister = sourcePersister;
	}
	
	public void addSelectCascade(Column sourcePrimaryKey,
								 Column relationOwner    // foreign key on target table
	) {
		// configuring select for fetching relation
		BeanRelationFixer<SRC, TRGT> relationFixer = BeanRelationFixer.of(
				manyRelationDefinition.getCollectionSetter(),
				manyRelationDefinition.getCollectionGetter(),
				manyRelationDefinition.getCollectionFactory(),
				Objects.preventNull(manyRelationDefinition.getReverseSetter(), NOOP_REVERSE_SETTER));
		
		String createdJoinNodeName = sourcePersister.addPersister(FIRST_STRATEGY_NAME,
				targetPersister,
				relationFixer,
				sourcePrimaryKey,
				relationOwner,
				relationOwner.isNullable()); // outer join for empty relation cases
		Column targetPrimaryKey = (Column) Iterables.first(targetPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		addSubgraphSelect(createdJoinNodeName, sourcePersister, targetPersister, sourcePrimaryKey, targetPrimaryKey, relationFixer, manyRelationDefinition.getCollectionGetter());
	}
	
	private static <SRC, TRGT, ID, C extends Collection<TRGT>> void addSubgraphSelect(String joinPoint, IEntityConfiguredJoinedTablesPersister<SRC, ID> sourcePersister,
																			 IConfiguredJoinedTablesPersister<TRGT, ID> targetPersister,
																			 Column leftColumn,
																			 Column rightColumn,
																			 BeanRelationFixer<SRC, TRGT> beanRelationFixer,
																			 Function<SRC, C> targetProvider) {
		// we add target subgraph joins to the one that was created
//		targetPersister.joinWith(sourcePersister, leftColumn, rightColumn, beanRelationFixer);
		targetPersister.copyJoinsRootTo(sourcePersister.getJoinedStrategiesSelect(), joinPoint);
//		targetPersister.getJoinedStrategiesSelect().getJoinsRoot().copyTo(sourcePersister.getJoinedStrategiesSelect(), joinPoint);
		
		// we must trigger subgraph event on loading of our own graph, this is mainly for event that initializes thngs because given ids
		// are not those of their entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, ID>() {
			@Override
			public void beforeSelect(Iterable<ID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}

			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				Iterable collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(targetProvider.apply(src))
						.map(Collection::stream)
						.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}

			@Override
			public void onError(Iterable<ID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	public void addInsertCascade() {
		sourcePersister.getPersisterListener().addInsertListener(
				new OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		BiConsumer<Duo<SRC, SRC>, Boolean> updateListener = new CollectionUpdater<>(
				manyRelationDefinition.getCollectionGetter(),
				targetPersister,
				manyRelationDefinition.getReverseSetter(),
				shouldDeleteRemoved);
		sourcePersister.getPersisterListener().addUpdateListener(
				new OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public void addDeleteCascade(boolean deleteTargetEntities) {
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			sourcePersister.getPersisterListener().addDeleteListener(
					new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.getPersisterListener().addDeleteByIdListener(
					new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
		} else // entity shouldn't be deleted, so we may have to update it
			if (manyRelationDefinition.getReverseSetter() != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				sourcePersister.getPersisterListener().addDeleteListener(new BeforeDeleteCollectionCascader<SRC, TRGT>(targetPersister) {
					
					@Override
					protected void postTargetDelete(Iterable<TRGT> entities) {
						// nothing to do after deletion
					}
					
					@Override
					public void beforeDelete(Iterable<SRC> entities) {
						List<TRGT> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> manyRelationDefinition.getReverseSetter().accept(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<TRGT> getTargets(SRC src) {
						Collection<TRGT> targets = manyRelationDefinition.getCollectionGetter().apply(src);
						// We only delete persisted instances (for logic and to prevent from non matching row count exception)
						return stream(targets)
								.filter(not(targetPersister.getMappingStrategy()::isNew))
								.collect(Collectors.toList());
					}
				});
			}
	}
	
	static class TargetInstancesInsertCascader<I, O, J> extends AfterInsertCollectionCascader<I, O> {
		
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
	
	static class TargetInstancesUpdateCascader<I, O> extends AfterUpdateCollectionCascader<I, O> {
		
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
	
	static class DeleteTargetEntitiesBeforeDeleteCascader<I, O> extends BeforeDeleteCollectionCascader<I, O> {
		
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
