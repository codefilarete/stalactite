package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IDeleteListener<T> {
	
	void beforeDelete(Iterable<T> iterables);
	
	void afterDelete(Iterable<T> iterables);
	
}
