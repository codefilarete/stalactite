package org.gama.stalactite.persistence.engine.listening;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class SelectListenerCollection<T> implements ISelectListener<T> {
	
	private List<ISelectListener<T>> selectListeners = new ArrayList<>();
	
	@Override
	public void beforeSelect(Serializable id) {
		for (ISelectListener<T> selectListener : selectListeners) {
			selectListener.beforeSelect(id);
		}
	}
	
	@Override
	public void afterSelect(T result) {
		for (ISelectListener<T> selectListener : selectListeners) {
			selectListener.afterSelect(result);
		}
	}
	
	public void add(ISelectListener<T> selectListener) {
		if (selectListener != null) {    // prevent null as specified in interface
			this.selectListeners.add(selectListener);
		}
	}
}
