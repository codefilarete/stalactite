package org.codefilarete.stalactite.engine.listener;

/**
 * @author Guillaume Mary
 */
public interface SelectListener<C, I> {
	
	default void beforeSelect(Iterable<I> ids) {
		
	}
	
	default void afterSelect(Iterable<? extends C> result) {
		
	}
	
	default void onSelectError(Iterable<I> ids, RuntimeException exception) {
		
	}
	
}
