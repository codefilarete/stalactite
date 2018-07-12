package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface IInsertListener<T> {
	
	void beforeInsert(Iterable<T> entities);
	
	void afterInsert(Iterable<T> entities);
	
	void onError(Iterable<T> entities, RuntimeException runtimeException);
	
}
