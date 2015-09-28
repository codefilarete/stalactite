package org.gama.stalactite.persistence.engine.listening;

import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public class NoopUpdateListener<T> implements IUpdateListener<T> {
	
	@Override
	public void beforeUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement) {
		
	}
	
	@Override
	public void afterUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement) {
		
	}
}
