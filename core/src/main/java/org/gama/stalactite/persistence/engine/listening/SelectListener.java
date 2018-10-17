package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface SelectListener<T, I> {
	
	default void beforeSelect(Iterable<I> ids) {
		
	}
	
	default void afterSelect(Iterable<T> result) {
		
	}
	
	default void onError(Iterable<I> ids, RuntimeException exception) {
		
	}
	
}
