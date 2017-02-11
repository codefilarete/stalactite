package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;

/**
 * Cascader for insert, written for @OneToMany style of cascade where Target owns the relationship to Trigger
 *
 * @author Guillaume Mary
 */
public abstract class AfterInsertCollectionCascader<Trigger, Target> extends NoopInsertListener<Trigger> {
	
	private Persister<Target, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	public AfterInsertCollectionCascader(Persister<Target, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addInsertListener(new NoopInsertListener<Target>() {
			@Override
			public void afterInsert(Iterable<Target> iterables) {
				super.afterInsert(iterables);
				postTargetInsert(iterables);
			}
		});
	}
	
	/**
	 * As supposed, since Trigger owns the relationship, we have to persist Target before Trigger instances insertion.
	 * So {@link IInsertListener#beforeInsert(Iterable)} is overriden.
	 *
	 * @param iterables
	 */
	@Override
	public void afterInsert(Iterable<Trigger> iterables) {
		this.persister.insert(Iterables.stream(iterables).flatMap(c -> getTargets(c).stream()).collect(Collectors.toList()));
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param iterables
	 */
	protected abstract void postTargetInsert(Iterable<Target> iterables);
	
	/**
	 * Expected to give the Target instances of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objets or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Collection<Target> getTargets(Trigger trigger);
	
}
