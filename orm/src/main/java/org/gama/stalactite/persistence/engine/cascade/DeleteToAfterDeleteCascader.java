package org.gama.stalactite.persistence.engine.cascade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IDeleteListener;
import org.gama.stalactite.persistence.engine.listening.NoopDeleteListener;

/**
 * Cascader for delete, written for @OneToOne style of cascade where Trigger owns the relationship with Target
 * 
 * @author Guillaume Mary
 */
public abstract class DeleteToAfterDeleteCascader<Trigger, Target> extends NoopDeleteListener<Trigger> {
	
	private Persister<Target, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	DeleteToAfterDeleteCascader(Persister<Target, ?> persister) {
		this.persister = persister;
		this.persister.getPersisterListener().addDeleteListener(new NoopDeleteListener<Target>() {
			@Override
			public void afterDelete(Iterable<Target> iterables) {
				super.afterDelete(iterables);
				postTargetDelete(iterables);
			}
		});
	}
	
	/**
	 * As supposed, since Trigger owns the relationship, we have to delete Target after Trigger instances deletion.
	 * So {@link IDeleteListener#afterDelete(Iterable)} is overriden.
	 *
	 * @param iterables
	 */
	@Override
	public void afterDelete(Iterable<Trigger> iterables) {
		List<Target> targets = new ArrayList<>(50);
		for (Trigger trigger : iterables) {
			targets.addAll(getTargets(trigger));
		}
		this.persister.delete(targets);
	}
	
	/**
	 * Post treatment after Target instance deletion. Cache cleanup for instance.
	 * 
	 * @param iterables
	 */
	protected abstract void postTargetDelete(Iterable<Target> iterables);
	
	/**
	 * Expected to give or create the corresponding Target instances of Trigger
	 * 
	 * @param trigger
	 * @return
	 */
	protected abstract Collection<Target> getTargets(Trigger trigger);
	
}
