package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IUpdateListener;
import org.gama.stalactite.persistence.engine.listening.NoopUpdateListener;

import java.util.*;

/**
 * Cascader for update, written for @OneToOne style of cascade where Trigger owns the relationship with Target
 *
 * @author Guillaume Mary
 */
public abstract class UpdateToAfterUpdateCascader<Trigger, Target> extends NoopUpdateListener<Trigger> {
	
	private Persister<Target> persister;
	private boolean allColumnsStatement;
	
	UpdateToAfterUpdateCascader(Persister<Target> persister, boolean allColumnsStatement) {
		this.persister = persister;
		this.persister.getPersisterListener().addUpdateListener(new NoopUpdateListener<Target>() {
			@Override
			public void afterUpdate(Iterable<Map.Entry<Target, Target>> iterables) {
				super.afterUpdate(iterables);
				postTargetUpdate(iterables);
			}
		});
		this.allColumnsStatement = allColumnsStatement;
	}
	
	/**
	 * Supposing Trigger owns the relationship, it seems more intuitive that Target updates happen after Trigger
	 * update. So {@link IUpdateListener#afterUpdate(Iterable)} is overriden.
	 *
	 * @param iterables
	 */
	@Override
	public void afterUpdate(Iterable<Map.Entry<Trigger, Trigger>> iterables) {
		List<Map.Entry<Target, Target>> targets = new ArrayList<>(50);
		for (Map.Entry<Trigger, Trigger> trigger : iterables) {
			Trigger modifiedTrigger = trigger.getKey();
			Trigger unmodifiedTrigger = trigger.getValue();
			Collection<Map.Entry<Target, Target>> modifiedTargets = getTargets(modifiedTrigger, unmodifiedTrigger);
			targets.addAll(modifiedTargets);
		}
		this.persister.update(targets, allColumnsStatement);
	}
	
	/**
	 * To override for cases where Target instances need to be adapted after their update.
	 *
	 * @param iterables
	 */
	protected abstract void postTargetUpdate(Iterable<Map.Entry<Target, Target>> iterables);
	
	/**
	 * Expected to give or create the corresponding Target instances of Trigger
	 *
	 * @param modifiedTrigger
	 * @param unmodifiedTrigger
	 * @return
	 */
	protected abstract Collection<Map.Entry<Target, Target>> getTargets(Trigger modifiedTrigger, Trigger unmodifiedTrigger);
	
}
