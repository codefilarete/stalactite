package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateRoughlyListenerCollection<T> implements IUpdateRoughlyListener<T> {
	
	private List<IUpdateRoughlyListener<T>> updateRouglyListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdateRoughly(Iterable<T> iterables) {
		for (IUpdateRoughlyListener<T> updateRouglyListener : updateRouglyListeners) {
			updateRouglyListener.beforeUpdateRoughly(iterables);
		}
	}
	
	@Override
	public void afterUpdateRoughly(Iterable<T> iterables) {
		for (IUpdateRoughlyListener<T> updateRouglyListener : updateRouglyListeners) {
			updateRouglyListener.afterUpdateRoughly(iterables);
		}
	}
	
	public void add(IUpdateRoughlyListener<T> updateRouglyListener) {
		if (updateRouglyListener != null) {    // prevent null as specified in interface
			this.updateRouglyListeners.add(updateRouglyListener);
		}
	}
}
