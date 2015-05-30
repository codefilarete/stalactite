package org.gama.stalactite.persistence.engine.listening;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public class NoopSelectListener<T> implements ISelectListener<T> {
	
	@Override
	public void beforeSelect(Serializable id) {
		
	}
	
	@Override
	public void afterSelect(T result) {
		
	}
}
