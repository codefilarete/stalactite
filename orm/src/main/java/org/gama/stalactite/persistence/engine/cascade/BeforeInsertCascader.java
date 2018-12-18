package org.gama.stalactite.persistence.engine.cascade;

import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;

/**
 * Cascader for insert, written for @OneToOne style of cascade where Trigger owns the relationship to Target
 *
 * @author Guillaume Mary
 */
public abstract class BeforeInsertCascader<Trigger, Target> implements InsertListener<Trigger> {
	
	private Persister<Target, ?, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	public BeforeInsertCascader(Persister<Target, ?, ?> persister) {
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
	public void beforeInsert(Iterable<? extends Trigger> entities) {
		this.persister.insert(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).collect(Collectors.toList()));
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param entities entities inserted by this listener
	 */
	protected abstract void postTargetInsert(Iterable<? extends Target> entities);
	
	/**
	 * Expected to give or create the corresponding Target instances of Trigger (should simply give a field)
	 *
	 * @param trigger
	 * @return
	 */
	protected abstract Target getTarget(Trigger trigger);
	
}
