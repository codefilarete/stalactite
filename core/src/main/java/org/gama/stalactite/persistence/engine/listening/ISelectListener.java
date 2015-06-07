package org.gama.stalactite.persistence.engine.listening;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public interface ISelectListener<T> {
	
	void beforeSelect(Iterable<Serializable> ids);
	
	void afterSelect(Iterable<T> result);
	
}
