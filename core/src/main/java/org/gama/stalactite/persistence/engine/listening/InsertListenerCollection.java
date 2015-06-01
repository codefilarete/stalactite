package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class InsertListenerCollection<T> implements IInsertListener<T> {
	
	private List<IInsertListener<T>> insertListeners = new ArrayList<>();
	
	@Override
	public void beforeInsert(Iterable<T> iterables) {
		for (IInsertListener<T> insertListener : insertListeners) {
			insertListener.beforeInsert(iterables);
		}
	}
	
	@Override
	public void afterInsert(Iterable<T> iterables) {
		for (IInsertListener<T> insertListener : insertListeners) {
			insertListener.afterInsert(iterables);
		}
	}
	
	public void add(IInsertListener<T> insertListener) {
		if (insertListener != null) {	// prevent null as specified in interface
			this.insertListeners.add(insertListener);
		}
	}
}
