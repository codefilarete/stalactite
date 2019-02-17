package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.collection.Iterables.stream;
import static org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.FIRST_STRATEGY_NAME;

/**
 * @author Guillaume Mary
 */
public class OneToManyWithMappedAssociationEngine<I extends Identified, O extends Identified, J extends Identifier, C extends Collection<O>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	protected static final BiConsumer NOOP_REVERSE_SETTER = (o, i) -> {
		/* Having a reverse setter in one to many relation with intermediary table isn't possible (cascadeMany.getReverseSetter() is null)
		 * because as soon as "mappedBy" is used (which fills reverseSetter), an intermediary table is not possible
		 */
	};
	
	protected final JoinedTablesPersister<I, J, ?> joinedTablesPersister;
	
	protected final PersisterListener<I, J> persisterListener;
	
	protected final Persister<O, J, ?> targetPersister;
	
	protected final MappedManyRelationDescriptor<I, O, C> manyRelationDefinition;
	
	public OneToManyWithMappedAssociationEngine(PersisterListener<I, J> persisterListener,
												Persister<O, J, ?> targetPersister,
												MappedManyRelationDescriptor<I, O, C> manyRelationDefinition,
												JoinedTablesPersister<I, J, ?> joinedTablesPersister) {
		this.persisterListener = persisterListener;
		this.targetPersister = targetPersister;
		this.manyRelationDefinition = manyRelationDefinition;
		this.joinedTablesPersister = joinedTablesPersister;
	}
	
	public void addSelectCascade(Column sourcePrimaryKey,
								 Column relationshipOwner    // foreign key on target table
	) {
		// configuring select for fetching relation
		BeanRelationFixer<I, O> relationFixer = BeanRelationFixer.of(
				manyRelationDefinition.getCollectionSetter(),
				manyRelationDefinition.getCollectionGetter(),
				manyRelationDefinition.getCollectionClass(),
				Objects.preventNull(manyRelationDefinition.getReverseSetter(), NOOP_REVERSE_SETTER));
		
		joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
				targetPersister,
				relationFixer,
				sourcePrimaryKey,
				relationshipOwner,
				true);
	}
	
	public void addInsertCascade() {
		persisterListener.addInsertListener(new OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener = new CollectionUpdater<>(
				manyRelationDefinition.getCollectionGetter(),
				targetPersister,
				manyRelationDefinition.getReverseSetter(),
				shouldDeleteRemoved);
		persisterListener.addUpdateListener(new OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public <T extends Table<T>> void addDeleteCascade(boolean deleteTargetEntities) {
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			persisterListener.addDeleteByIdListener(new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, manyRelationDefinition.getCollectionGetter()));
		} else // entity shouldn't be deleted, so we may have to update it
			if (manyRelationDefinition.getReverseSetter() != null) {
				// we cut the link between target and source
				// NB : we don't take versioning into account overall because we can't : how to do it since we miss the unmodified version ?
				persisterListener.addDeleteListener(new BeforeDeleteCollectionCascader<I, O>(targetPersister) {
					
					@Override
					protected void postTargetDelete(Iterable<O> entities) {
						// nothing to do after deletion
					}
					
					@Override
					public void beforeDelete(Iterable<I> entities) {
						List<O> targets = stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList());
						targets.forEach(e -> manyRelationDefinition.getReverseSetter().accept(e, null));
						targetPersister.updateById(targets);
					}
					
					@Override
					protected Collection<O> getTargets(I i) {
						Collection<O> targets = manyRelationDefinition.getCollectionGetter().apply(i);
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
		protected Collection<O> getTargets(I source) {
			Collection<O> targets = collectionGetter.apply(source);
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
	
	static class DeleteTargetEntitiesBeforeDeleteCascader<I extends Identified, O extends Identified> extends BeforeDeleteCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteTargetEntitiesBeforeDeleteCascader(Persister<O, ?, ?> targetPersister, Function<I, ? extends Collection<O>> collectionGetter) {
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
					.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
					.collect(Collectors.toList());
		}
	}
	
	static class DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<I extends Identified, O extends Identified> extends BeforeDeleteByIdCollectionCascader<I, O> {
		
		private final Function<I, ? extends Collection<O>> collectionGetter;
		
		public DeleteByIdTargetEntitiesBeforeDeleteByIdCascader(Persister<O, ?, ?> targetPersister,
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
					.filter(CascadeOneConfigurer.PERSISTED_PREDICATE)
					.collect(Collectors.toList());
		}
	}
}