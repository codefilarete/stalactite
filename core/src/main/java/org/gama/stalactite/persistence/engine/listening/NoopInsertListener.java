package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public class NoopInsertListener<T> implements IInsertListener<T> {
	
	@Override
	public void beforeInsert(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterInsert(Iterable<T> iterables) {
		
	}
}
