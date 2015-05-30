package org.gama.stalactite.persistence.engine.listening;

import java.io.Serializable;

/**
 * @author Guillaume Mary
 */
public interface ISelectListener<T> {
	
	void beforeSelect(Serializable id);
	
	void afterSelect(T result);
	
}
