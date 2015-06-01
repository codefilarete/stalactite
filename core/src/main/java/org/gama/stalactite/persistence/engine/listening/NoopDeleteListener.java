package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopDeleteListener<T> implements IDeleteListener<T> {
	@Override
	public void beforeDelete(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterDelete(Iterable<T> iterables) {
		
	}
}
