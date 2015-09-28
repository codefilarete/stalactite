package org.gama.stalactite.persistence.engine.listening;

import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public interface IUpdateListener<T> {
	
	void beforeUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement);
	
	void afterUpdate(Iterable<Entry<T, T>> iterables, boolean allColumnsStatement);
	
}
