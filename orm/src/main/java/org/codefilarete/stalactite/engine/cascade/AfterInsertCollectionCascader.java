package org.codefilarete.stalactite.engine.cascade;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;

/**
 * Cascader for insert, written for one-to-many style of cascade where Target owns the relation to Trigger
 *
 * @param <TRIGGER> type of the elements that trigger this collection cascade
 * @param <TARGET> collection elements type
 * @author Guillaume Mary
 */
public abstract class AfterInsertCollectionCascader<TRIGGER, TARGET> implements InsertListener<TRIGGER> {
	
	private final EntityReadWriteExecutor<TARGET, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to {@link PersisterListenerCollection} afterward.
	 *
	 * @param persister the entity write executor responsible for persisting target entities
	 */
	public AfterInsertCollectionCascader(EntityReadWriteExecutor<TARGET, ?> persister) {
		this.persister = persister;
		this.persister.addInsertListener(new InsertListener<TARGET>() {
			@Override
			public void afterInsert(Iterable<? extends TARGET> entities) {
				postTargetInsert(entities);
			}
		});
	}
	
	public EntityWriteExecutor<TARGET, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overridden to insert Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously inserted
	 */
	@Override
	public void afterInsert(Iterable<? extends TRIGGER> entities) {
		List<TARGET> targetEntities = Iterables.stream(entities)
				.flatMap(c -> Objects.preventNull(getTargets(c), Collections.<TARGET>emptySet()).stream())
				.collect(Collectors.toList());
		this.persister.persist(targetEntities);
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param entities entities inserted by this listener
	 */
	protected abstract void postTargetInsert(Iterable<? extends TARGET> entities);
	
	/**
	 * Expected to give the Target instances of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objets or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<TARGET> getTargets(TRIGGER trigger);
	
}
