package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopDeleteByIdListener<T> implements IDeleteByIdListener<T> {
	
	@Override
	public void beforeDeleteById(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterDeleteById(Iterable<T> iterables) {
		
	}
}
