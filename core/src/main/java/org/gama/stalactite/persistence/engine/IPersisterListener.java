package org.gama.stalactite.persistence.engine;

import java.io.Serializable;
import java.util.Map.Entry;

/**
 * @author Guillaume Mary
 */
public interface IPersisterListener<T> {
	
	void beforeInsert(Iterable<T> iterables);
	
	void afterInsert(Iterable<T> iterables);
	
	void beforeUpdateRoughly(Iterable<T> iterables);
	
	void afterUpdateRoughly(Iterable<T> iterables);
	
	void beforeUpdate(Iterable<Entry<T, T>> iterables);
	
	void afterUpdate(Iterable<Entry<T, T>> iterables);
	
	void beforeDelete(Iterable<T> iterables);
	
	void afterDelete(Iterable<T> iterables);
	
	void beforeSelect(Serializable id);
	
	void afterSelect(T result);
}
