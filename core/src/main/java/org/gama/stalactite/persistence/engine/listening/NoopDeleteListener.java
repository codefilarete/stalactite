package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopDeleteListener<T> implements IDeleteListener<T> {
	@Override
	public void beforeDelete(Iterable<T> entities) {
		
	}
	
	@Override
	public void afterDelete(Iterable<T> entities) {
		
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
