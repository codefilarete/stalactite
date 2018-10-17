package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface InsertListener<T> {
	
	default void beforeInsert(Iterable<T> entities) {
		
	}
	
	default void afterInsert(Iterable<T> entities) {
		
	}
	
	default void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
	
}
