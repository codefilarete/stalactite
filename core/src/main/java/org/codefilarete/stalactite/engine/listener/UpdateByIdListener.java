package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface UpdateByIdListener<C> {
	
	default void beforeUpdateById(Iterable<? extends C> entities) {
		
	}
	
	default void afterUpdateById(Iterable<? extends C> entities) {
		
	}
	
	default void onError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
}
