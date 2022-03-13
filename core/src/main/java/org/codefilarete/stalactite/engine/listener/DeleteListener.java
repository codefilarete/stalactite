package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface DeleteListener<C> {
	
	default void beforeDelete(Iterable<C> entities) {
		
	}
	
	default void afterDelete(Iterable<C> entities) {
		
	}
	
	default void onError(Iterable<C> entities, RuntimeException runtimeException) {
		
	}
	
}
