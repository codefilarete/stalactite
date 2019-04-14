package org.gama.stalactite.persistence.engine.cascade;

import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.InsertListener;

/**
 * Cascader for insert, written for one-to-one style of cascade where Trigger owns the relationship to Target : Targets must be inserted before Triggers
 * to get their primary keys and then put them as the foreign key in Triggers
 *
 * @param <TRIGGER> type of the elements that trigger this cascade
 * @param <TARGET> relationship entity type
 * @author Guillaume Mary
 */
public abstract class BeforeInsertCascader<TRIGGER, TARGET> implements InsertListener<TRIGGER> {
	
	private final Persister<TARGET, ?, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	public BeforeInsertCascader(Persister<TARGET, ?, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addInsertListener(new InsertListener<TARGET>() {
			@Override
			public void afterInsert(Iterable<? extends TARGET> entities) {
				postTargetInsert(entities);
			}
		});
	}
	
	public Persister<TARGET, ?, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to insert Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously inserted
	 */
	@Override
	public void beforeInsert(Iterable<? extends TRIGGER> entities) {
		this.persister.insert(Iterables.stream(entities).map(this::getTarget).filter(Objects::nonNull).collect(Collectors.toList()));
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param entities entities inserted by this listener
	 */
	protected abstract void postTargetInsert(Iterable<? extends TARGET> entities);
	
	/**
	 * Expected to give or create the corresponding Target instances of Trigger (should simply give a field)
	 *
	 * @param trigger
	 * @return
	 */
	protected abstract TARGET getTarget(TRIGGER trigger);
	
}
