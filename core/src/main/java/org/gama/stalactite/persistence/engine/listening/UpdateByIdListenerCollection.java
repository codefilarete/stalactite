package org.gama.stalactite.persistence.engine.listening;

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
		if (updateByIdListener != null) {    // prevent null as specified in interface
			this.updateByIdListeners.add(updateByIdListener);
		}
	}
}
