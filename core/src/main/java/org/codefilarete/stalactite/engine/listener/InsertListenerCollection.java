package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class InsertListenerCollection<C> implements InsertListener<C> {
	
	private final List<InsertListener<C>> insertListeners = new ArrayList<>();
	
	@Override
	public void beforeInsert(Iterable<? extends C> entities) {
		insertListeners.forEach(listener -> listener.beforeInsert(entities));
	}
	
	@Override
	public void afterInsert(Iterable<? extends C> entities) {
		insertListeners.forEach(listener -> listener.afterInsert(entities));
	}
	
	@Override
	public void onInsertError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		insertListeners.forEach(listener -> listener.onInsertError(entities, runtimeException));
	}
	
	public void add(InsertListener<? extends C> insertListener) {
		this.insertListeners.add((InsertListener<C>) insertListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param insertListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(InsertListenerCollection<C> insertListener) {
		insertListener.insertListeners.addAll(this.insertListeners);
		this.insertListeners.clear();
	}
}
