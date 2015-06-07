package org.gama.stalactite.persistence.engine.listening;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public class NoopSelectListener<T> implements ISelectListener<T> {
	
	@Override
	public void beforeSelect(Iterable<Serializable> ids) {
		
	}
	
	@Override
	public void afterSelect(Iterable<T> result) {
		
	}
}
