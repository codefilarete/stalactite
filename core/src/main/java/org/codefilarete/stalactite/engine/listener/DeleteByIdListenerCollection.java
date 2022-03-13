package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteByIdListenerCollection<T> implements DeleteByIdListener<T> {
	
	private List<DeleteByIdListener<T>> deleteByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeDeleteById(Iterable<T> entities) {
		deleteByIdListeners.forEach(listener -> listener.beforeDeleteById(entities));
	}
	
	@Override
	public void afterDeleteById(Iterable<T> entities) {
		deleteByIdListeners.forEach(listener -> listener.afterDeleteById(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		deleteByIdListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(DeleteByIdListener<T> deleteByIdListener) {
		this.deleteByIdListeners.add(deleteByIdListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Usefull to agregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param deleteByIdListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(DeleteByIdListenerCollection<T> deleteByIdListener) {
		deleteByIdListener.deleteByIdListeners.addAll(this.deleteByIdListeners);
		this.deleteByIdListeners.clear();
	}
	
}
