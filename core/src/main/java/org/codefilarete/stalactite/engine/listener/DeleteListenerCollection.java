package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteListenerCollection<C> implements DeleteListener<C> {
	
	private final List<DeleteListener<C>> deleteListeners = new ArrayList<>();
	
	@Override
	public void beforeDelete(Iterable<? extends C> entities) {
		deleteListeners.forEach(listener -> listener.beforeDelete(entities));
	}
	
	@Override
	public void afterDelete(Iterable<? extends C> entities) {
		deleteListeners.forEach(listener -> listener.afterDelete(entities));
	}
	
	@Override
	public void onDeleteError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		deleteListeners.forEach(listener -> listener.onDeleteError(entities, runtimeException));
	}
	
	public void add(DeleteListener<? extends C> deleteListener) {
		this.deleteListeners.add((DeleteListener<C>) deleteListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param deleteListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(DeleteListenerCollection<C> deleteListener) {
		deleteListener.deleteListeners.addAll(this.deleteListeners);
		this.deleteListeners.clear();
	}
	
}
