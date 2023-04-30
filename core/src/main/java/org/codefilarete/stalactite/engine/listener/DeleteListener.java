package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface DeleteListener<C> {
	
	default void beforeDelete(Iterable<? extends C> entities) {
		
	}
	
	default void afterDelete(Iterable<? extends C> entities) {
		
	}
	
	default void onError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
	
}
