package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopSelectListener<T, I> implements ISelectListener<T, I> {
	
	@Override
	public void beforeSelect(Iterable<I> ids) {
		
	}
	
	@Override
	public void afterSelect(Iterable<T> result) {
		
	}
}
