package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class DeleteByIdListenerCollection<T> implements IDeleteByIdListener<T> {
	
	private List<IDeleteByIdListener<T>> deleteByIdListeners = new ArrayList<>();
	
	@Override
	public void beforeDeleteById(Iterable<T> iterables) {
		for (IDeleteByIdListener<T> deleteByIdListener : deleteByIdListeners) {
			deleteByIdListener.beforeDeleteById(iterables);
		}
	}
	
	@Override
	public void afterDeleteById(Iterable<T> iterables) {
		for (IDeleteByIdListener<T> deleteByIdListener : deleteByIdListeners) {
			deleteByIdListener.afterDeleteById(iterables);
		}
	}
	
	public void add(IDeleteByIdListener<T> deleteByIdListener) {
		if (deleteByIdListener != null) {    // prevent null as specified in interface
			this.deleteByIdListeners.add(deleteByIdListener);
		}
	}
}
