package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopUpdateByIdListener<T> implements IUpdateByIdListener<T> {
	
	@Override
	public void beforeUpdateById(Iterable<T> entities) {
		
	}
	
	@Override
	public void afterUpdateById(Iterable<T> entities) {
		
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
