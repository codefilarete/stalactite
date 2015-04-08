package org.stalactite.persistence.engine;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public class NoopPersisterListener<T> implements IPersisterListener<T> {
	@Override
	public void beforeInsert(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterInsert(Iterable<T> iterables) {
		
	}
	
	@Override
	public void beforeUpdate(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdate(Iterable<T> iterables) {
		
	}
	
	@Override
	public void beforeDelete(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterDelete(Iterable<T> iterables) {
		
	}
	
	@Override
	public void beforeSelect(Serializable id) {
		
	}
	
	@Override
	public void afterSelect(T result) {
		
	}
}
