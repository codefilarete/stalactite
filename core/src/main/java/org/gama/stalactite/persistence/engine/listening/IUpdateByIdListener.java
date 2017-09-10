package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IUpdateByIdListener<T> {
	
	void beforeUpdateById(Iterable<T> iterables);
	
	void afterUpdateById(Iterable<T> iterables);
	
	
}
