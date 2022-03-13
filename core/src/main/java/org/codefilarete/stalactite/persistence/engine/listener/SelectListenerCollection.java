package org.codefilarete.stalactite.persistence.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class SelectListenerCollection<T, I> implements SelectListener<T, I> {
	
	private List<SelectListener<T, I>> selectListeners = new ArrayList<>();
	
	@Override
	public void beforeSelect(Iterable<I> ids) {
		selectListeners.forEach(listener -> listener.beforeSelect(ids));
	}
	
	@Override
	public void afterSelect(Iterable<? extends T> entities) {
		selectListeners.forEach(listener -> listener.afterSelect(entities));
	}
	
	@Override
	public void onError(Iterable<I> ids, RuntimeException exception) {
		selectListeners.forEach(listener -> listener.onError(ids, exception));
	}
	
	public void add(SelectListener<T, I> selectListener) {
		this.selectListeners.add(selectListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Usefull to agregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param selectListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(SelectListenerCollection<T, I> selectListener) {
		selectListener.selectListeners.addAll(this.selectListeners);
		this.selectListeners.clear();
	}
	
}
