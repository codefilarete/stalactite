package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateByIdListenerCollection<T> implements UpdateByIdListener<T> {
	
	private List<UpdateByIdListener<T>> updateByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdateById(Iterable<T> entities) {
		updateByIdListeners.forEach(listener -> listener.beforeUpdateById(entities));
	}
	
	@Override
	public void afterUpdateById(Iterable<T> entities) {
		updateByIdListeners.forEach(listener -> listener.afterUpdateById(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		updateByIdListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(UpdateByIdListener<T> updateByIdListener) {
		this.updateByIdListeners.add(updateByIdListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param updateByIdListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(UpdateByIdListenerCollection<T> updateByIdListener) {
		updateByIdListener.updateByIdListeners.addAll(this.updateByIdListeners);
		this.updateByIdListeners.clear();
	}
	
}
