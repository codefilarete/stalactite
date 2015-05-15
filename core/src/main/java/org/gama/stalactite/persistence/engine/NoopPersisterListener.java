package org.gama.stalactite.persistence.engine;

import java.io.Serializable;
import java.util.Map.Entry;

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
	public void beforeUpdateRoughly(Iterable<T> iterables) {
		
	}
	
	@Override
	public void afterUpdateRoughly(Iterable<T> iterables) {
		
	}
	
	@Override
	public void beforeUpdate(Iterable<Entry<T, T>> iterables) {
		
	}
	
	@Override
	public void afterUpdate(Iterable<Entry<T, T>> iterables) {
		
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
