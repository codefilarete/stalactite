package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;

/**
 * Cascader for insert, written for @OneToMany style of cascade where Target owns the relationship to Trigger
 *
 * @author Guillaume Mary
 */
public abstract class AfterInsertCollectionCascader<Trigger, Target> implements InsertListener<Trigger> {
	
	private final Persister<Target, ?, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	public AfterInsertCollectionCascader(Persister<Target, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addInsertListener(new InsertListener<Target>() {
			@Override
			public void afterInsert(Iterable<? extends Target> entities) {
				postTargetInsert(entities);
			}
		});
	}
	
	/**
	 * Overriden to insert Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously inserted
	 */
	@Override
	public void afterInsert(Iterable<? extends Trigger> entities) {
		Stream<? extends Trigger> stream = Iterables.stream(entities);
		Stream<? extends Target> targetStream = stream.flatMap(c -> getTargets(c).stream());
		List<? extends Target> collect = targetStream.collect(Collectors.toList());
		this.persister.insert(collect);
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param entities entities inserted by this listener
	 */
	protected abstract void postTargetInsert(Iterable<? extends Target> entities);
	
	/**
	 * Expected to give the Target instances of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objets or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<Target> getTargets(Trigger trigger);
	
}
