package org.codefilarete.stalactite.engine.listener;

public interface PersistListener<C> {
	
	default void beforePersist(Iterable<? extends C> entities) {
		
	}
	
	default void afterPersist(Iterable<? extends C> entities) {
		
	}
	
	default void onPersistError(Iterable<? extends C> entities, RuntimeException runtimeException) {
		
	}
}
