package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteByIdListenerCollection<T> implements DeleteByIdListener<T> {
	
	private List<DeleteByIdListener<T>> deleteByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeDeleteById(Iterable<T> entities) {
		deleteByIdListeners.forEach(listener -> listener.beforeDeleteById(entities));
	}
	
	@Override
	public void afterDeleteById(Iterable<T> entities) {
		deleteByIdListeners.forEach(listener -> listener.afterDeleteById(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		deleteByIdListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(DeleteByIdListener<T> deleteByIdListener) {
		this.deleteByIdListeners.add(deleteByIdListener);
	}
}
