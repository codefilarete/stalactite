package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface DeleteByIdListener<T> {
	
	default void beforeDeleteById(Iterable<T> entities) {
		
	}
	
	default void afterDeleteById(Iterable<T> entities) {
		
	}
	
	default void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
