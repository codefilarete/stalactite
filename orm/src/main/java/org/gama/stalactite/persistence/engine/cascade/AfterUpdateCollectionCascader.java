package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;

/**
 * Cascader for update, written for @OneToOne style of cascade where Trigger owns the relationship to Target.
 * Target instances are updated after Trigger instances
 *
 * @param <Trigger> the type of the source that triggered the event
 * @param <Target> the type of the instances of the relationship
 * @author Guillaume Mary
 */
public abstract class AfterUpdateCollectionCascader<Trigger, Target> implements UpdateListener<Trigger> {
	
	private final Persister<Target, ?, ?> persister;
	
	public AfterUpdateCollectionCascader(Persister<Target, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addUpdateListener(new UpdateListener<Target>() {
			@Override
			public void afterUpdate(Iterable<UpdatePayload<? extends Target, ?>> entities, boolean allColumnsStatement) {
				postTargetUpdate(entities);
			}
		});
	}
	
	/**
	 * Overriden to update Target instances of the Trigger instances.
	 *  @param entities source entities previously updated
	 * @param allColumnsStatement
	 */
	@Override
	public void afterUpdate(Iterable<UpdatePayload<? extends Trigger, ?>> entities, boolean allColumnsStatement) {
		this.persister.update(Iterables.stream(entities).flatMap(e -> getTargets(e.getEntities().getLeft(), e.getEntities().getRight()).stream()).filter(Objects::nonNull)
				.collect(Collectors.toList()), allColumnsStatement);
	}
	
	/**
	 * To override for cases where Target instances need to be adapted after their update.
	 *
	 * @param entities entities updated by this listener
	 */
	protected abstract void postTargetUpdate(Iterable<UpdatePayload<? extends Target, ?>> entities);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 * 
	 * @param modifiedTrigger the source instance from which to take the target
	 * @param unmodifiedTrigger the source instance from which to take the target
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<Duo<Target, Target>> getTargets(Trigger modifiedTrigger, Trigger unmodifiedTrigger);
	
}
