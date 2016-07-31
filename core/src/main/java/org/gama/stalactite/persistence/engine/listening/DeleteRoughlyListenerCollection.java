package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteRoughlyListenerCollection<T> implements IDeleteRoughlyListener<T> {
	
	private List<IDeleteRoughlyListener<T>> deleteRouglyListeners = new ArrayList<>();
	
	@Override
	public void beforeDeleteRoughly(Iterable<T> iterables) {
		for (IDeleteRoughlyListener<T> updateRouglyListener : deleteRouglyListeners) {
			updateRouglyListener.beforeDeleteRoughly(iterables);
		}
	}
	
	@Override
	public void afterDeleteRoughly(Iterable<T> iterables) {
		for (IDeleteRoughlyListener<T> updateRouglyListener : deleteRouglyListeners) {
			updateRouglyListener.afterDeleteRoughly(iterables);
		}
	}
	
	public void add(IDeleteRoughlyListener<T> updateRouglyListener) {
		if (updateRouglyListener != null) {    // prevent null as specified in interface
			this.deleteRouglyListeners.add(updateRouglyListener);
		}
	}
}
