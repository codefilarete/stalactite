package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface DeleteByIdListener<C> {
	
	default void beforeDeleteById(Iterable<? extends C> entities) {
		
	}
	
	default void afterDeleteById(Iterable<? extends C> entities) {
		
	}
	
	default void onError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
}
