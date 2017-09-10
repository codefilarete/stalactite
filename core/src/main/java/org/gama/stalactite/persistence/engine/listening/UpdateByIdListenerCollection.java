package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateByIdListenerCollection<T> implements IUpdateByIdListener<T> {
	
	private List<IUpdateByIdListener<T>> updateByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdateById(Iterable<T> iterables) {
		for (IUpdateByIdListener<T> updateByIdListener : updateByIdListeners) {
			updateByIdListener.beforeUpdateById(iterables);
		}
	}
	
	@Override
	public void afterUpdateById(Iterable<T> iterables) {
		for (IUpdateByIdListener<T> updateByIdListener : updateByIdListeners) {
			updateByIdListener.afterUpdateById(iterables);
		}
	}
	
	public void add(IUpdateByIdListener<T> updateByIdListener) {
		if (updateByIdListener != null) {    // prevent null as specified in interface
			this.updateByIdListeners.add(updateByIdListener);
		}
	}
}
