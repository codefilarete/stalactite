package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;

/**
 * Cascader for delete, written for one-to-many style of cascade where Target owns the relationship to Trigger
 *
 * @param <TRIGGER> type of the elements that trigger this collection cascade
 * @param <TARGET> collection elements type
 * @author Guillaume Mary
 */
public abstract class BeforeDeleteByIdCollectionCascader<TRIGGER, TARGET> implements DeleteByIdListener<TRIGGER> {
	
	private final IEntityPersister<TARGET, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public BeforeDeleteByIdCollectionCascader(IEntityPersister<TARGET, ?> persister) {
		this.persister = persister;
		this.persister.addDeleteByIdListener(new DeleteByIdListener<TARGET>() {
			@Override
			public void afterDeleteById(Iterable<TARGET> entities) {
				postTargetDelete(entities);
			}
		});
	}
	
	public IEntityPersister<TARGET, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to delete Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously deleted
	 */
	@Override
	public void beforeDeleteById(Iterable<TRIGGER> entities) {
		this.persister.deleteById(Iterables.stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList()));
	}
	
	/**
	 * Post treatment after Target instance deletion. Cache cleanup for instance.
	 * 
	 * @param entities entities deleted by this listener
	 */
	protected abstract void postTargetDelete(Iterable<TARGET> entities);
	
	/**
	 * Expected to give the Target instances of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objets or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<TARGET> getTargets(TRIGGER trigger);
	
}
