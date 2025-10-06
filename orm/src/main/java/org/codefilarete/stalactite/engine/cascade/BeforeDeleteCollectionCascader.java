package org.codefilarete.stalactite.engine.cascade;

import java.util.Collection;
import java.util.stream.Collectors;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;

/**
 * Cascader for delete, written for one-to-many style of cascade where Target owns the relation to Trigger
 *
 * @param <TRIGGER> type of the elements that trigger this collection cascade
 * @param <TARGET> collection elements type
 * @author Guillaume Mary
 */
public abstract class BeforeDeleteCollectionCascader<TRIGGER, TARGET> implements DeleteListener<TRIGGER> {
	
	private final EntityPersister<TARGET, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to {@link PersisterListenerCollection} afterward.
	 * @param persister
	 */
	public BeforeDeleteCollectionCascader(EntityPersister<TARGET, ?> persister) {
		this.persister = persister;
	}
	
	public EntityPersister<TARGET, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overridden to delete Target instances of the Trigger instances.
	 * 
	 * @param entities source entities previously deleted
	 */
	@Override
	public void beforeDelete(Iterable<? extends TRIGGER> entities) {
		this.persister.delete(Iterables.stream(entities).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList()));
	}
	
	/**
	 * Expected to give the Target instances of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objets or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<TARGET> getTargets(TRIGGER trigger);
	
}
