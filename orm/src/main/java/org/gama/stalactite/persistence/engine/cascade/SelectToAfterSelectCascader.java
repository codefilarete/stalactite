package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.ISelectListener;
import org.gama.stalactite.persistence.engine.listening.NoopSelectListener;

/**
 * Cascader for select, written for @OneToOne style of cascade where Trigger owns the relationship with Target.
 * Use carefully : this class triggers selects for Target instances so it will result in a N+1 symptom. Prefer using a join select.
 * 
 * @author Guillaume Mary
 */
public abstract class SelectToAfterSelectCascader<Trigger, Target, I> extends NoopSelectListener<Trigger, I> {
	
	private Persister<Target, I> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public SelectToAfterSelectCascader(Persister<Target, I> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addSelectListener(new NoopSelectListener<Target, I>() {
			@Override
			public void afterSelect(Iterable<Target> iterable) {
				super.afterSelect(iterable);
				postTargetSelect(iterable);
			}
		});
	}
	
	/**
	 * As supposed, since Trigger owns the relationship, we have to select Target after Trigger instances select.
	 * So {@link ISelectListener#afterSelect(Iterable)} is overriden.
	 * Overriden to select the Target instance corresponding to the Trigger instance.
	 *
	 * @param iterables
	 */
	@Override
	public void afterSelect(Iterable<Trigger> iterables) {
		List<I> targets = new ArrayList<>(50);
		for (Trigger trigger : iterables) {
			targets.addAll(getTargetIds(trigger));
		}
		this.persister.select(targets);
	}
	
	/**
	 * Post treatment after Target instance select. Cache addition for instance.
	 *
	 * @param iterable
	 */
	protected abstract void postTargetSelect(Iterable<Target> iterable);
	
	/**
	 * Expected to give the corresponding Target identifier of Trigger
	 * 
	 * @param trigger
	 * @return
	 */
	protected abstract Collection<I> getTargetIds(Trigger trigger);
	
}
