package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteByIdListenerCollection<C> implements DeleteByIdListener<C> {
	
	private final List<DeleteByIdListener<C>> deleteByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeDeleteById(Iterable<? extends C> entities) {
		deleteByIdListeners.forEach(listener -> listener.beforeDeleteById(entities));
	}
	
	@Override
	public void afterDeleteById(Iterable<? extends C> entities) {
		deleteByIdListeners.forEach(listener -> listener.afterDeleteById(entities));
	}
	
	@Override
	public void onError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		deleteByIdListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(DeleteByIdListener<? extends C> deleteByIdListener) {
		this.deleteByIdListeners.add((DeleteByIdListener<C>) deleteByIdListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param deleteByIdListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(DeleteByIdListenerCollection<C> deleteByIdListener) {
		deleteByIdListener.deleteByIdListeners.addAll(this.deleteByIdListeners);
		this.deleteByIdListeners.clear();
	}
	
}
