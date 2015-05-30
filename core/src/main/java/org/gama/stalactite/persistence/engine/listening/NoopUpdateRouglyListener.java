package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopUpdateRouglyListener<T> implements IUpdateRouglyListener<T> {
	
	@Override
	public void beforeUpdateRoughly(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdateRoughly(Iterable<T> iterables) {
		
	}
}
