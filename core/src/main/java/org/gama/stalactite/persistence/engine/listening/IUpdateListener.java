package org.gama.stalactite.persistence.engine.listening;

import org.gama.lang.Duo;

/**
 * @author Guillaume Mary
 */
public interface IUpdateListener<T> {
	
	void beforeUpdate(Iterable<Duo<T, T>> iterables, boolean allColumnsStatement);
	
	void afterUpdate(Iterable<Duo<T, T>> iterables, boolean allColumnsStatement);
	
	void onError(Iterable<T> entities, RuntimeException runtimeException);
	
}
