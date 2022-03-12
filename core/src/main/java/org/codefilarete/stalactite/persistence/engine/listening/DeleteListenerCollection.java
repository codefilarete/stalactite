package org.codefilarete.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteListenerCollection<T> implements DeleteListener<T> {
	
	private List<DeleteListener<T>> deleteListeners = new ArrayList<>();
	
	@Override
	public void beforeDelete(Iterable<T> entities) {
		deleteListeners.forEach(listener -> listener.beforeDelete(entities));
	}
	
	@Override
	public void afterDelete(Iterable<T> entities) {
		deleteListeners.forEach(listener -> listener.afterDelete(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		deleteListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(DeleteListener<T> deleteListener) {
		this.deleteListeners.add(deleteListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Usefull to agregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param deleteListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(DeleteListenerCollection<T> deleteListener) {
		deleteListener.deleteListeners.addAll(this.deleteListeners);
		this.deleteListeners.clear();
	}
	
}
