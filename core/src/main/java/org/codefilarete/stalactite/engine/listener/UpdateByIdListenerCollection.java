package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateByIdListenerCollection<C> implements UpdateByIdListener<C> {
	
	private final List<UpdateByIdListener<C>> updateByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdateById(Iterable<? extends C> entities) {
		updateByIdListeners.forEach(listener -> listener.beforeUpdateById(entities));
	}
	
	@Override
	public void afterUpdateById(Iterable<? extends C> entities) {
		updateByIdListeners.forEach(listener -> listener.afterUpdateById(entities));
	}
	
	@Override
	public void onUpdateError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		updateByIdListeners.forEach(listener -> listener.onUpdateError(entities, runtimeException));
	}
	
	public void add(UpdateByIdListener<C> updateByIdListener) {
		this.updateByIdListeners.add(updateByIdListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param updateByIdListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(UpdateByIdListenerCollection<C> updateByIdListener) {
		updateByIdListener.updateByIdListeners.addAll(this.updateByIdListeners);
		this.updateByIdListeners.clear();
	}
	
}
