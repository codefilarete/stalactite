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
		updateByIdListeners.forEach(listener -> listener.beforeUpdateById(iterables));
	}
	
	@Override
	public void afterUpdateById(Iterable<T> iterables) {
		updateByIdListeners.forEach(listener -> listener.afterUpdateById(iterables));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		updateByIdListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(IUpdateByIdListener<T> updateByIdListener) {
		if (updateByIdListener != null) {    // prevent null as specified in interface
			this.updateByIdListeners.add(updateByIdListener);
		}
	}
}
