package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopInsertListener<T> implements IInsertListener<T> {
	
	@Override
	public void beforeInsert(Iterable<T> entities) {
		
	}
	
	@Override
	public void afterInsert(Iterable<T> entities) {
		
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
