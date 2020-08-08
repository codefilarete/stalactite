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
	
	/**
	 * Move internal listeners to given instance.
	 * Usefull to agregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param insertListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(InsertListenerCollection<T> insertListener) {
		insertListener.insertListeners.addAll(this.insertListeners);
		this.insertListeners.clear();
	}
}
