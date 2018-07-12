package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
public interface ISelectListener<T, I> {
	
	void beforeSelect(Iterable<I> ids);
	
	void afterSelect(Iterable<T> result);
	
	void onError(Iterable<I> ids, RuntimeException exception);
	
}
