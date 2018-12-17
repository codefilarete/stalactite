package org.gama.stalactite.persistence.engine.listening;

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
}
