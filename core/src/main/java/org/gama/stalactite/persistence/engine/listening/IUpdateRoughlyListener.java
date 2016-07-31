package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IUpdateRoughlyListener<T> {
	
	void beforeUpdateRoughly(Iterable<T> iterables);
	
	void afterUpdateRoughly(Iterable<T> iterables);
	
	
}
