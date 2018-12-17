package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class InsertListenerCollection<T> implements InsertListener<T> {
	
	private List<InsertListener<T>> insertListeners = new ArrayList<>();
	
	@Override
	public void beforeInsert(Iterable<? extends T> entities) {
		insertListeners.forEach(listener -> listener.beforeInsert(entities));
	}
	
	@Override
	public void afterInsert(Iterable<? extends T> entities) {
		insertListeners.forEach(listener -> listener.afterInsert(entities));
	}
	
	@Override
	public void onError(Iterable<? extends T> entities, RuntimeException runtimeException) {
		insertListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(InsertListener<T> insertListener) {
		this.insertListeners.add(insertListener);
	}
}
