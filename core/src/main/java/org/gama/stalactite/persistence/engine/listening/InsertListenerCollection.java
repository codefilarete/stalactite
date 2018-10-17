package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class InsertListenerCollection<T> implements InsertListener<T> {
	
	private List<InsertListener<T>> insertListeners = new ArrayList<>();
	
	@Override
	public void beforeInsert(Iterable<T> entities) {
		insertListeners.forEach(listener -> listener.beforeInsert(entities));
	}
	
	@Override
	public void afterInsert(Iterable<T> entities) {
		insertListeners.forEach(listener -> listener.afterInsert(entities));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		insertListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(InsertListener<T> insertListener) {
		if (insertListener != null) {	// prevent null as specified in interface
			this.insertListeners.add(insertListener);
		}
	}
}
