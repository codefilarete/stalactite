package org.codefilarete.stalactite.engine.cascade;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.bean.Objects.preventNull;

/**
 * Cascader for update, written for one-to-many style of cascade where Trigger owns the relation to Target.
 * Target instances are updated after Trigger instances
 *
 * @param <TRIGGER> type of the elements that trigger this cascade
 * @param <TARGET> relation entity type
 * @author Guillaume Mary
 */
public abstract class AfterUpdateCollectionCascader<TRIGGER, TARGET> implements UpdateListener<TRIGGER> {
	
	private final EntityPersister<TARGET, ?> persister;
	
	public AfterUpdateCollectionCascader(EntityPersister<TARGET, ?> persister) {
		this.persister = persister;
		this.persister.addUpdateListener(new UpdateListener<TARGET>() {
			@Override
			public void afterUpdate(Iterable<? extends Duo<TARGET, TARGET>> entities, boolean allColumnsStatement) {
				postTargetUpdate(entities);
			}
		});
	}
	
	public EntityPersister<TARGET, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overridden to update Target instances of the Trigger instances.
	 * @param entities source entities previously updated
	 * @param allColumnsStatement true if all columns must be updated, false if only changed ones must be in the update statement
	 */
	@Override
	public void afterUpdate(Iterable<? extends Duo<TRIGGER, TRIGGER>> entities, boolean allColumnsStatement) {
		List<Duo<TARGET, TARGET>> targetEntities = Iterables.stream(entities)
				.flatMap(e -> preventNull(getTargets(e.getLeft(), e.getRight()), Collections.<Duo<TARGET, TARGET>>emptySet()).stream())
				.collect(Collectors.toList());
		this.persister.update(targetEntities, allColumnsStatement);
	}
	
	/**
	 * To override for cases where Target instances need to be adapted after their update.
	 *
	 * @param entities entities updated by this listener
	 */
	protected abstract void postTargetUpdate(Iterable<? extends Duo<? extends TARGET, ? extends TARGET>> entities);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 * 
	 * @param modifiedTrigger the source instance from which to take the target
	 * @param unmodifiedTrigger the source instance from which to take the target
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<Duo<TARGET, TARGET>> getTargets(TRIGGER modifiedTrigger, TRIGGER unmodifiedTrigger);
	
}
