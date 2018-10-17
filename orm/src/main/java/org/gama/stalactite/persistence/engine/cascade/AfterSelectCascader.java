package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.SelectListener;

/**
 * Cascader for select, written for @OneToOne style of cascade where Trigger owns the relationship with Target.
 * Use carefully : this class triggers selects for Target instances so it will result in a N+1 symptom. Prefer using a join select.
 * 
 * @param <Trigger> source type that triggers this listener
 * @param <Target> type of loaded beans
 * @param <I> type of loaded beans key
 * @author Guillaume Mary
 */
public abstract class AfterSelectCascader<Trigger, Target, I> implements SelectListener<Trigger, I> {
	
	private Persister<Target, I, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public AfterSelectCascader(Persister<Target, I, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addSelectListener(new SelectListener<Target, I>() {
			@Override
			public void afterSelect(Iterable<Target> entities) {
				postTargetSelect(entities);
			}
		});
	}
	
	/**
	 * Overriden to select Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously selected
	 */
	@Override
	public void afterSelect(Iterable<Trigger> entities) {
		List<I> targets = new ArrayList<>(50);
		for (Trigger trigger : entities) {
			targets.addAll(getTargetIds(trigger));
		}
		this.persister.select(targets);
	}
	
	/**
	 * Post treatment after Target instance select. Cache addition for instance.
	 *
	 * @param entities entities selected by this listener
	 */
	protected abstract void postTargetSelect(Iterable<Target> entities);
	
	/**
	 * Expected to give the corresponding Target identifier of Trigger
	 * 
	 * @param trigger
	 * @return
	 */
	protected abstract Collection<I> getTargetIds(Trigger trigger);
	
}
