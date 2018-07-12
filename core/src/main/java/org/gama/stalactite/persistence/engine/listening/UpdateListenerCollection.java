package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public class UpdateListenerCollection<T> implements IUpdateListener<T> {
	
	private List<IUpdateListener<T>> updateListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.beforeUpdate(iterables, allColumnsStatement));
	}
	
	@Override
	public void afterUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.afterUpdate(iterables, allColumnsStatement));
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		updateListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(IUpdateListener<T> updateListener) {
		if (updateListener != null) {    // prevent null as specified in interface
			this.updateListeners.add(updateListener);
		}
	}
}
