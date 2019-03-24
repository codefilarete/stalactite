package org.gama.stalactite.persistence.engine.cascade;

import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;

/**
 * Cascader for delete, written for one-to-one style of cascade where Trigger owns the relationship to Target
 *
 * @param <TRIGGER> type of the elements that trigger this cascade
 * @param <TARGET> relationship entity type
 * @author Guillaume Mary
 */
public abstract class AfterDeleteCascader<TRIGGER, TARGET> implements DeleteListener<TRIGGER> {
	
	private final Persister<TARGET, ?, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to {@link PersisterListener} afterward.
	 * @param persister
	 */
	public AfterDeleteCascader(Persister<TARGET, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addDeleteListener(new DeleteListener<TARGET>() {
			@Override
			public void afterDelete(Iterable<TARGET> entities) {
				postTargetDelete(entities);
			}
		});
	}
	
	public Persister<TARGET, ?, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to delete Target instances of the Trigger instances.
	 * 
	 * @param entities source entities previously deleted
	 */
	@Override
	public void afterDelete(Iterable<TRIGGER> entities) {
		this.persister.delete(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).collect(Collectors.toList()));
	}
	
	/**
	 * Post treatment after Target instance deletion. Cache cleanup for instance.
	 * 
	 * @param entities entities deleted by this listener
	 */
	protected abstract void postTargetDelete(Iterable<TARGET> entities);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract TARGET getTarget(TRIGGER trigger);
	
}
