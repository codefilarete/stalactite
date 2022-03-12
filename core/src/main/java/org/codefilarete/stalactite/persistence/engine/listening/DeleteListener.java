package org.codefilarete.stalactite.persistence.engine.listening;

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
