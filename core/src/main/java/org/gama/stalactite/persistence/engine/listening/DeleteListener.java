package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface DeleteListener<T> {
	
	default void beforeDelete(Iterable<T> entities) {
		
	}
	
	default void afterDelete(Iterable<T> entities) {
		
	}
	
	default void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
	
}
