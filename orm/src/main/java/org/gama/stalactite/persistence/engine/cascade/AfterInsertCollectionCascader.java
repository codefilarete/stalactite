package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.IEntityPersister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;

/**
 * Cascader for insert, written for one-to-many style of cascade where Target owns the relationship to Trigger
 *
 * @param <TRIGGER> type of the elements that trigger this collection cascade
 * @param <TARGET> collection elements type
 * @author Guillaume Mary
 */
public abstract class AfterInsertCollectionCascader<TRIGGER, TARGET> implements InsertListener<TRIGGER> {
	
	private final IEntityPersister<TARGET, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	public AfterInsertCollectionCascader(IEntityPersister<TARGET, ?> persister) {
		this.persister = persister;
		this.persister.addInsertListener(new InsertListener<TARGET>() {
			@Override
			public void afterInsert(Iterable<? extends TARGET> entities) {
				postTargetInsert(entities);
			}
		});
	}
	
	public IEntityPersister<TARGET, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to insert Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously inserted
	 */
	@Override
	public void afterInsert(Iterable<? extends TRIGGER> entities) {
		Stream<TRIGGER> stream = Iterables.stream(entities);
		Stream<TARGET> targetStream = stream.flatMap(c -> getTargets(c).stream());
		// We collect things in a Set to avoid persisting duplicates twice which may produce constraint exception : the Set is an identity Set to
		// avoid basing our comparison on implemented equals/hashCode although this could be sufficent, identity seems safer and match our logic.
		Set<TARGET> collect = targetStream.collect(Collectors.toCollection(Collections::newIdentitySet));
		this.persister.persist(collect);
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
