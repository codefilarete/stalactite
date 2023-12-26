package org.codefilarete.stalactite.engine.listener;

import java.util.Set;

/**
 * @author Guillaume Mary
 */
public interface SelectListener<C, I> {
	
	default void beforeSelect(Iterable<I> ids) {
		
	}
	
	default void afterSelect(Set<? extends C> result) {
		
	}
	
	default void onSelectError(Iterable<I> ids, RuntimeException exception) {
		
	}
	
}
