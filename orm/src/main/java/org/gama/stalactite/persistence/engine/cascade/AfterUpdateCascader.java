package org.gama.stalactite.persistence.engine.cascade;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateListener;

/**
 * Cascader for update, written for @OneToOne style of cascade where Trigger owns the relationship to Target.
 * Target instances are updated after Trigger instances
 *
 * @param <Trigger> the type of the source that triggered the event
 * @param <Target> the type of the instances of the relationship
 * @author Guillaume Mary
 */
public abstract class AfterUpdateCascader<Trigger, Target> extends NoopUpdateListener<Trigger> {
	
	private Persister<Target, ?> persister;
	
	public AfterUpdateCascader(Persister<Target, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addUpdateListener(new NoopUpdateListener<Target>() {
			@Override
			public void afterUpdate(Iterable<Map.Entry<Target, Target>> iterables, boolean allColumnsStatement) {
				super.afterUpdate(iterables, allColumnsStatement);
				postTargetUpdate(iterables);
			}
		});
	}
	
	/**
	 * Supposing Trigger owns the relationship, it seems more intuitive that Target updates happen after Trigger
	 * updates. So {@link IUpdateListener#afterUpdate(Iterable, boolean)} is overriden.
	 *
	 * @param iterables
	 */
	@Override
	public void afterUpdate(Iterable<Map.Entry<Trigger, Trigger>> iterables, boolean allColumnsStatement) {
		this.persister.update(Iterables.stream(iterables).map(e -> getTarget(e.getKey(), e.getValue())).filter(Objects::nonNull)
				.collect(Collectors.toList()), allColumnsStatement);
	}
	
	/**
	 * To override for cases where Target instances need to be adapted after their update.
	 *
	 * @param iterables
	 */
	protected abstract void postTargetUpdate(Iterable<Map.Entry<Target, Target>> iterables);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 * 
	 * @param modifiedTrigger the source instance from which to take the target
	 * @param unmodifiedTrigger the source instance from which to take the target
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Map.Entry<Target, Target> getTarget(Trigger modifiedTrigger, Trigger unmodifiedTrigger);
	
}
