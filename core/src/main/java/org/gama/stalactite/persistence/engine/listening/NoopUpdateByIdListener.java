package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopUpdateByIdListener<T> implements IUpdateByIdListener<T> {
	
	@Override
	public void beforeUpdateById(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdateById(Iterable<T> iterables) {
		
	}
}
