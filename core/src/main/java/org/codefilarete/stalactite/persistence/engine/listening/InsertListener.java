package org.codefilarete.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface InsertListener<C> {
	
	default void beforeInsert(Iterable<? extends C> entities) {
		
	}
	
	default void afterInsert(Iterable<? extends C> entities) {
		
	}
	
	default void onError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
	
}
