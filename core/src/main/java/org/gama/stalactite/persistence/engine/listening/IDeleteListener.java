package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IDeleteListener<T> {
	
	void beforeDelete(Iterable<T> entities);
	
	void afterDelete(Iterable<T> entities);
	
	void onError(Iterable<T> entities, RuntimeException runtimeException);
	
}
