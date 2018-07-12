package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IDeleteByIdListener<T> {
	
	void beforeDeleteById(Iterable<T> entities);
	
	void afterDeleteById(Iterable<T> entities);
	
	void onError(Iterable<T> entities, RuntimeException runtimeException);
}
