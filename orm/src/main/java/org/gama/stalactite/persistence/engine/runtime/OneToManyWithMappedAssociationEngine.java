package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
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
	
	/** Empty setter for applying source entity to target entity (reverse side). Available only when association is mapped without intermediary table */
	static final SerializableBiConsumer NOOP_REVERSE_SETTER = (o, i) -> {
		/* Having a reverse setter in one to many relation with intermediary table isn't possible (cascadeMany.getReverseSetter() is null)
		 * because as soon as "mappedBy" is used (which fills reverseSetter), an intermediary table is not possible
		 */
	};
	
	/** Setter for applying source entity to reverse side (target entity). Available only when association is mapped without intermediary table */
	protected final BiConsumer<O, I> reverseSetter;
	
	protected final JoinedTablesPersister<I, J, ?> joinedTablesPersister;
	
	protected final PersisterListener<I, J> persisterListener;
	
	protected final Persister<O, J, ?> targetPersister;
	
	protected final Function<I, C> collectionGetter;
	
	public OneToManyWithMappedAssociationEngine(PersisterListener<I, J> persisterListener,
														Persister<O, J, ?> targetPersister,
														Function<I, C> collectionGetter,
														BiConsumer<O, I> reverseSetter,
														JoinedTablesPersister<I, J, ?> joinedTablesPersister) {
		this.persisterListener = persisterListener;
		this.targetPersister = targetPersister;
		this.collectionGetter = collectionGetter;
		this.reverseSetter = reverseSetter;
		this.joinedTablesPersister = joinedTablesPersister;
	}
	
	public void addSelectCascade(CascadeMany<I, O, J, C> cascadeMany,
								 Column sourcePrimaryKey,
								 Column relationshipOwner	// foreign key on target table
	) {
		
		IMutator<I, C> collectionSetter = Accessors.<I, C>of(cascadeMany.getMember()).getMutator();
		// configuring select for fetching relation
		SerializableBiConsumer<O, I> reverseMember = Objects.preventNull(cascadeMany.getReverseSetter(), NOOP_REVERSE_SETTER);
		
		BeanRelationFixer relationFixer = newRelationFixer(cascadeMany, collectionSetter, reverseMember, persisterListener);
		
		joinedTablesPersister.addPersister(FIRST_STRATEGY_NAME,
				targetPersister,
				relationFixer,
				sourcePrimaryKey,
				relationshipOwner,
				true);
	}
	
	protected BeanRelationFixer newRelationFixer(CascadeMany<I, O, J, C> cascadeMany, IMutator<I, C> collectionSetter,
												 SerializableBiConsumer<O, I> reverseMember, PersisterListener<I, J> persisterListener) {
		BeanRelationFixer relationFixer = BeanRelationFixer.of(collectionSetter::set, collectionGetter,
				cascadeMany.getCollectionTargetClass(), reverseMember);
		return relationFixer;
	}
	
	public void addInsertCascade() {
		persisterListener.addInsertListener(new OneToManyWithMappedAssociationEngine.TargetInstancesInsertCascader<>(targetPersister, collectionGetter));
	}
	
	public void addUpdateCascade(boolean shouldDeleteRemoved) {
		BiConsumer<UpdatePayload<? extends I, ?>, Boolean> updateListener = new CollectionUpdater<>(collectionGetter, targetPersister, reverseSetter,
				shouldDeleteRemoved);
		persisterListener.addUpdateListener(new OneToManyWithMappedAssociationEngine.TargetInstancesUpdateCascader<>(targetPersister, updateListener));
	}
	
	public <T extends Table<T>> void addDeleteCascade(boolean deleteTargetEntities) {
		if (deleteTargetEntities) {
			// adding deletion of many-side entities
			persisterListener.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(targetPersister, collectionGetter));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			persisterListener.addDeleteByIdListener(new DeleteByIdTargetEntitiesBeforeDeleteByIdCascader<>(targetPersister, collectionGetter));
		} else // entity shouldn't be deleted, so we may have to update it
			if (reverseSetter != null) {
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
