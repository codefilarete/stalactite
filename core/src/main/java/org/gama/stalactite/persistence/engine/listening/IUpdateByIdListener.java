package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IUpdateByIdListener<T> {
	
	void beforeUpdateById(Iterable<T> entities);
	
	void afterUpdateById(Iterable<T> entities);
	
	void onError(Iterable<T> entities, RuntimeException runtimeException);
}
