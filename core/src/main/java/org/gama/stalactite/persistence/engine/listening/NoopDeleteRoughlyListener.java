package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopDeleteRoughlyListener<T> implements IDeleteRoughlyListener<T> {
	
	@Override
	public void beforeDeleteRoughly(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterDeleteRoughly(Iterable<T> iterables) {
		
	}
}
