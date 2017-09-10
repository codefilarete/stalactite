package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IDeleteByIdListener<T> {
	
	void beforeDeleteById(Iterable<T> iterables);
	
	void afterDeleteById(Iterable<T> iterables);
	
}
