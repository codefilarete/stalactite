package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteListenerCollection<T> implements IDeleteListener<T> {
	
	private List<IDeleteListener<T>> deleteListeners = new ArrayList<>();
	
	@Override
	public void beforeDelete(Iterable<T> entities) {
		deleteListeners.forEach(listener -> listener.beforeDelete(entities));
	}
	
	@Override
	public void afterDelete(Iterable<T> entities) {
		deleteListeners.forEach(listener -> listener.afterDelete(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		deleteListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(IDeleteListener<T> deleteListener) {
		if (deleteListener != null) {    // prevent null as specified in interface
			this.deleteListeners.add(deleteListener);
		}
	}
}
