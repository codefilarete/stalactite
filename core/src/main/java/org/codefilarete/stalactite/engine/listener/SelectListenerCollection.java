package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Guillaume Mary
 */
public class SelectListenerCollection<C, I> implements SelectListener<C, I> {
	
	private final List<SelectListener<C, I>> selectListeners = new ArrayList<>();
	
	@Override
	public void beforeSelect(Iterable<I> ids) {
		selectListeners.forEach(listener -> listener.beforeSelect(ids));
	}
	
	@Override
	public void afterSelect(Set<? extends C> entities) {
		selectListeners.forEach(listener -> listener.afterSelect(entities));
	}
	
	@Override
	public void onSelectError(Iterable<I> ids, RuntimeException exception) {
		selectListeners.forEach(listener -> listener.onSelectError(ids, exception));
	}
	
	public void add(SelectListener<? extends C, I> selectListener) {
		this.selectListeners.add((SelectListener<C, I>) selectListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param selectListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(SelectListenerCollection<C, I> selectListener) {
		selectListener.selectListeners.addAll(this.selectListeners);
		this.selectListeners.clear();
	}
	
}
