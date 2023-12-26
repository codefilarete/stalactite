package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface DeleteByIdListener<C> {
	
	default void beforeDeleteById(Iterable<? extends C> entities) {
		
	}
	
	default void afterDeleteById(Iterable<? extends C> entities) {
		
	}
	
	default void onDeleteError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
}
