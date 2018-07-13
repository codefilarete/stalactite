package org.gama.stalactite.persistence.engine.listening;

import org.gama.lang.Duo;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopUpdateListener<T> implements IUpdateListener<T> {
	
	@Override
	public void beforeUpdate(Iterable<Duo<T, T>> iterables, boolean allColumnsStatement) {
		
	}
	
	@Override
	public void afterUpdate(Iterable<Duo<T, T>> iterables, boolean allColumnsStatement) {
		
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
