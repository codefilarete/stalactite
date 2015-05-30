package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.listening.IDeleteListener;

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
