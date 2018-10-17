package org.gama.stalactite.persistence.engine.cascade;

import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;

/**
 * Cascader for delete, written for @OneToOne style of cascade where Trigger owns the relationship to Target
 * 
 * @author Guillaume Mary
 */
public abstract class AfterDeleteByIdCascader<Trigger, Target> implements DeleteByIdListener<Trigger> {
	
	private Persister<Target, ?, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public AfterDeleteByIdCascader(Persister<Target, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addDeleteByIdListener(new DeleteByIdListener<Target>() {
			@Override
			public void afterDeleteById(Iterable<Target> entities) {
				postTargetDelete(entities);
			}
		});
	}
	
	/**
	 * Overriden to delete Target instances of the Trigger instances.
	 * 
	 * @param entities source entities previously deleted
	 */
	@Override
	public void afterDeleteById(Iterable<Trigger> entities) {
		this.persister.deleteById(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).collect(Collectors.toList()));
	}
	
	/**
	 * Post treatment after Target instance deletion. Cache cleanup for instance.
	 * 
	 * @param entities entities deleted by this listener
	 */
	protected abstract void postTargetDelete(Iterable<Target> entities);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Target getTarget(Trigger trigger);
	
}
