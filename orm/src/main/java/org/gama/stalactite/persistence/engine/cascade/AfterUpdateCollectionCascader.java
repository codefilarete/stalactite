package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;

/**
 * Cascader for update, written for one-to-one style of cascade where Trigger owns the relationship to Target.
 * Target instances are updated after Trigger instances
 *
 * @param <TRIGGER> type of the elements that trigger this cascade
 * @param <TARGET> relationship entity type
 * @author Guillaume Mary
 */
public abstract class AfterUpdateCollectionCascader<TRIGGER, TARGET> implements UpdateListener<TRIGGER> {
	
	private final Persister<TARGET, ?, ?> persister;
	
	public AfterUpdateCollectionCascader(Persister<TARGET, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addUpdateListener(new UpdateListener<TARGET>() {
			@Override
			public void afterUpdate(Iterable<UpdatePayload<? extends TARGET, ?>> entities, boolean allColumnsStatement) {
				postTargetUpdate(entities);
			}
		});
	}
	
	public Persister<TARGET, ?, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to update Target instances of the Trigger instances.
	 * @param entities source entities previously updated
	 * @param allColumnsStatement true if all columns must be updated, false if only changed ones must be in the update statement
	 */
	@Override
	public void afterUpdate(Iterable<UpdatePayload<? extends TRIGGER, ?>> entities, boolean allColumnsStatement) {
		this.persister.update(Iterables.stream(entities).flatMap(e -> getTargets(e.getEntities().getLeft(), e.getEntities().getRight()).stream()).filter(Objects::nonNull)
				.collect(Collectors.toList()), allColumnsStatement);
	}
	
	/**
	 * To override for cases where Target instances need to be adapted after their update.
	 *
	 * @param entities entities updated by this listener
	 */
	protected abstract void postTargetUpdate(Iterable<UpdatePayload<? extends TARGET, ?>> entities);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 * 
	 * @param modifiedTrigger the source instance from which to take the target
	 * @param unmodifiedTrigger the source instance from which to take the target
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<Duo<TARGET, TARGET>> getTargets(TRIGGER modifiedTrigger, TRIGGER unmodifiedTrigger);
	
}
