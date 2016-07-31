package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopUpdateRoughlyListener<T> implements IUpdateRoughlyListener<T> {
	
	@Override
	public void beforeUpdateRoughly(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdateRoughly(Iterable<T> iterables) {
		
	}
}
