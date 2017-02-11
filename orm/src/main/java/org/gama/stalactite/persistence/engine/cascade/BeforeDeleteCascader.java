package org.gama.stalactite.persistence.engine.cascade;

import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.listening.IDeleteListener;
import org.gama.stalactite.persistence.engine.listening.NoopDeleteListener;

/**
 * Cascader for delete, written for @OneToOne style of cascade where Trigger owns the relationship to Target
 * 
 * @author Guillaume Mary
 */
public abstract class BeforeDeleteCascader<Trigger, Target> extends NoopDeleteListener<Trigger> {
	
	private Persister<Target, ?> persister;
	
	/**
	 * Simple constructor. Created instance must be added to PersisterListener afterward.
	 * @param persister
	 */
	public BeforeDeleteCascader(Persister<Target, ?> persister) {
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
	public void beforeDelete(Iterable<Trigger> iterables) {
		this.persister.delete(Iterables.stream(iterables).map(this::getTarget).filter(Objects::nonNull).collect(Collectors.toList()));
	}
	
	/**
	 * Post treatment after Target instance deletion. Cache cleanup for instance.
	 * 
	 * @param iterables
	 */
	protected abstract void postTargetDelete(Iterable<Target> iterables);
	
	/**
	 * Expected to give the Target instance of a Trigger (should simply give a field value of trigger)
	 *
	 * @param trigger the source instance from which to take the targets
	 * @return the linked objet or null if there's not (or shouldn't be persisted for whatever reason)
	 */
	protected abstract Target getTarget(Trigger trigger);
	
}
