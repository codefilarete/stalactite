package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;

/**
 * Cascader for insert, written for @OneToOne style of cascade where Trigger owns the relationship with Target
 *
 * @author Guillaume Mary
 */
public abstract class InsertToBeforeInsertCascader<Trigger, Target> extends NoopInsertListener<Trigger> {
	
	private Persister<Target, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 *
	 * @param persister
	 */
	InsertToBeforeInsertCascader(Persister<Target, ?> persister) {
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
	public void beforeInsert(Iterable<Trigger> iterables) {
		List<Target> targets = new ArrayList<>(50);
		for (Trigger trigger : iterables) {
			targets.addAll(getTargets(trigger));
		}
		this.persister.insert(targets);
	}
	
	/**
	 * Expected to adapt Target instances after their insertion. For instance set the owner property on Trigger instances
	 * or apply bidirectionnal mapping with Trigger.
	 *
	 * @param iterables
	 */
	protected abstract void postTargetInsert(Iterable<Target> iterables);
	
	/**
	 * Expected to give or create the corresponding Target instances of Trigger (should simply give a field)
	 *
	 * @param trigger
	 * @return
	 */
	protected abstract Collection<Target> getTargets(Trigger trigger);
	
}
