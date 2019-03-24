package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.SelectListener;

/**
 * Cascader for select, written for one-to-one style of cascade where Trigger owns the relationship to Target.
 * Use carefully : this class triggers selects for Target instances so it will result in a N+1 symptom. Prefer using a join select.
 *
 * @param <TRIGGER> type of the elements that trigger this cascade
 * @param <TARGET> relationship entity type
 * @param <TARGETID> relationship entity identifier type
 * @author Guillaume Mary
 */
public abstract class AfterSelectCascader<TRIGGER, TARGET, TARGETID> implements SelectListener<TRIGGER, TARGETID> {
	
	private final Persister<TARGET, TARGETID, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public AfterSelectCascader(Persister<TARGET, TARGETID, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addSelectListener(new SelectListener<TARGET, TARGETID>() {
			@Override
			public void afterSelect(Iterable<? extends TARGET> entities) {
				postTargetSelect(entities);
			}
		});
	}
	
	public Persister<TARGET, TARGETID, ?> getPersister() {
		return persister;
	}
	
	/**
	 * Overriden to select Target instances of the Trigger instances.
	 *
	 * @param entities source entities previously selected
	 */
	@Override
	public void afterSelect(Iterable<? extends TRIGGER> entities) {
		List<TARGETID> targets = new ArrayList<>(50);
		for (TRIGGER trigger : entities) {
			targets.addAll(getTargetIds(trigger));
		}
		this.persister.select(targets);
	}
	
	/**
	 * Post treatment after Target instance select. Cache addition for instance.
	 *
	 * @param entities entities selected by this listener
	 */
	protected abstract void postTargetSelect(Iterable<? extends TARGET> entities);
	
	/**
	 * Expected to give the corresponding Target identifier of Trigger
	 * 
	 * @param trigger
	 * @return
	 */
	protected abstract Collection<TARGETID> getTargetIds(TRIGGER trigger);
	
}
