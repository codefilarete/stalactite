package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class SelectListenerCollection<T, I> implements ISelectListener<T, I> {
	
	private List<ISelectListener<T, I>> selectListeners = new ArrayList<>();
	
	@Override
	public void beforeSelect(Iterable<I> ids) {
		for (ISelectListener<T, I> selectListener : selectListeners) {
			selectListener.beforeSelect(ids);
		}
	}
	
	@Override
	public void afterSelect(Iterable<T> result) {
		for (ISelectListener<T, I> selectListener : selectListeners) {
			selectListener.afterSelect(result);
		}
	}
	
	public void add(ISelectListener<T, I> selectListener) {
		if (selectListener != null) {    // prevent null as specified in interface
			this.selectListeners.add(selectListener);
		}
	}
}
