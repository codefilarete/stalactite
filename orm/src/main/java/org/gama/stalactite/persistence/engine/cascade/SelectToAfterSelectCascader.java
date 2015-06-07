package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.ISelectListener;
import org.gama.stalactite.persistence.engine.listening.NoopSelectListener;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Cascader for select, written for @OneToOne style of cascade where Trigger owns the relationship with Target.
 * To be used carefully because if this class is overriden to trigger other Select for Target instances it will result
 * in a N+1 symptom.
 * 
 * @author Guillaume Mary
 */
public abstract class SelectToAfterSelectCascader<Trigger, Target> extends NoopSelectListener<Trigger> {
	
	private Persister<Target> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	SelectToAfterSelectCascader(Persister<Target> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addSelectListener(new NoopSelectListener<Target>() {
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
		List<Serializable> targets = new ArrayList<>(50);
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
	protected abstract Collection<Serializable> getTargetIds(Trigger trigger);
	
}
