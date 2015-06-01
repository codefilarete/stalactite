package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteListenerCollection<T> implements IDeleteListener<T> {
	
	private List<IDeleteListener<T>> deleteListeners = new ArrayList<>();
	
	@Override
	public void beforeDelete(Iterable<T> iterables) {
		for (IDeleteListener<T> deleteListener : deleteListeners) {
			deleteListener.beforeDelete(iterables);
		}
	}
	
	@Override
	public void afterDelete(Iterable<T> iterables) {
		for (IDeleteListener<T> deleteListener : deleteListeners) {
			deleteListener.afterDelete(iterables);
		}
	}
	
	public void add(IDeleteListener<T> deleteListener) {
		if (deleteListener != null) {    // prevent null as specified in interface
			this.deleteListeners.add(deleteListener);
		}
	}
}
