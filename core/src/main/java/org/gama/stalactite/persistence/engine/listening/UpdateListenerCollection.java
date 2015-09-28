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
		for (IUpdateListener<T> updateListener : updateListeners) {
			updateListener.beforeUpdate(iterables, allColumnsStatement);
		}
	}
	
	@Override
	public void afterUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement) {
		for (IUpdateListener<T> updateListener : updateListeners) {
			updateListener.afterUpdate(iterables, allColumnsStatement);
		}
	}
	
	public void add(IUpdateListener<T> updateListener) {
		if (updateListener != null) {    // prevent null as specified in interface
			this.updateListeners.add(updateListener);
		}
	}
}
