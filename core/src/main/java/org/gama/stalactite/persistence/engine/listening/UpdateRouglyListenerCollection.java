package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateRouglyListenerCollection<T> implements IUpdateRouglyListener<T> {
	
	private List<IUpdateRouglyListener<T>> updateRouglyListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdateRoughly(Iterable<T> iterables) {
		for (IUpdateRouglyListener<T> updateRouglyListener : updateRouglyListeners) {
			updateRouglyListener.beforeUpdateRoughly(iterables);
		}
	}
	
	@Override
	public void afterUpdateRoughly(Iterable<T> iterables) {
		for (IUpdateRouglyListener<T> updateRouglyListener : updateRouglyListeners) {
			updateRouglyListener.afterUpdateRoughly(iterables);
		}
	}
	
	public void add(IUpdateRouglyListener<T> updateRouglyListener) {
		if (updateRouglyListener != null) {    // prevent null as specified in interface
			this.updateRouglyListeners.add(updateRouglyListener);
		}
	}
}
