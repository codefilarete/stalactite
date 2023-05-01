package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface InsertListener<C> {
	
	default void beforeInsert(Iterable<? extends C> entities) {
		
	}
	
	default void afterInsert(Iterable<? extends C> entities) {
		
	}
	
	default void onInsertError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
	
}
