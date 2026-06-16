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
	
	/**
	 * Combines the current {@code SelectListener} with another one, creating a new {@code SelectListener}
	 * that executes all operations of the current listener followed by the operations of the provided listener.
	 * Therefore, the listener returned by this method must be added to a {@link PersisterListener}, not this instance. 
	 *
	 * @param next the {@code SelectListener} to chain as the next step to execute
	 * @return a new {@code SelectListener} that first delegates to the current listener and then to the provided one
	 */
	default SelectListener<C, I> then(SelectListener<C, I> next) {
		return new SelectListener<C, I>() {
			@Override
			public void beforeSelect(Iterable<I> ids) {
				SelectListener.this.beforeSelect(ids);
				next.beforeSelect(ids);
			}
			
			@Override
			public void afterSelect(Set<? extends C> result) {
				SelectListener.this.afterSelect(result);
				next.afterSelect(result);
			}
			
			@Override
			public void onSelectError(Iterable<I> ids, RuntimeException exception) {
				SelectListener.this.onSelectError(ids, exception);
				next.onSelectError(ids, exception);
			}
		};
	}
	
}
