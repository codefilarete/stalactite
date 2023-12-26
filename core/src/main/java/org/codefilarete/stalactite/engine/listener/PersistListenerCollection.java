package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

public class PersistListenerCollection<C> implements PersistListener<C> {
	
	private final List<PersistListener<C>> persistListeners = new ArrayList<>();
	
	public List<PersistListener<C>> getPersistListeners() {
		return persistListeners;
	}
	
	@Override
	public void beforePersist(Iterable<? extends C> entities) {
		persistListeners.forEach(listener -> listener.beforePersist(entities));
	}
	
	@Override
	public void afterPersist(Iterable<? extends C> entities) {
		persistListeners.forEach(listener -> listener.afterPersist(entities));
	}
	
	@Override
	public void onPersistError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		persistListeners.forEach(listener -> listener.onPersistError(entities, runtimeException));
	}
	
	public void add(PersistListener<? extends C> persistListener) {
		this.persistListeners.add((PersistListener<C>) persistListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param persisTListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(PersistListenerCollection<C> persisTListener) {
		persisTListener.persistListeners.addAll(this.persistListeners);
		this.persistListeners.clear();
	}
}
