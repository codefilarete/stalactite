package org.stalactite.persistence.engine;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public interface IPersisterListener<T> {
	
	void beforeInsert(Iterable<T> iterables);
	
	void afterInsert(Iterable<T> iterables);
	
	void beforeUpdate(Iterable<T> iterables);
	
	void afterUpdate(Iterable<T> iterables);
	
	void beforeDelete(Iterable<T> iterables);
	
	void afterDelete(Iterable<T> iterables);
	
	void beforeSelect(Serializable id);
	
	void afterSelect(T result);
}
