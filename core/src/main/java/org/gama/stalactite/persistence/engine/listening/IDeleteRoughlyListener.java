package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IDeleteRoughlyListener<T> {
	
	void beforeDeleteRoughly(Iterable<T> iterables);
	
	void afterDeleteRoughly(Iterable<T> iterables);
	
}
