package org.gama.stalactite.persistence.engine.listening;

/**
 * @author Guillaume Mary
 */
@SuppressWarnings("squid:S1186")	// methods are obviously empty because it is the goal of this class
public class NoopUpdateListener<T> implements IUpdateListener<T> {
	
	@Override
	public void beforeUpdate(Iterable<UpdatePayload<T, ?>> updatePayloads, boolean allColumnsStatement) {
		
	}
	
	@Override
	public void afterUpdate(Iterable<UpdatePayload<T, ?>> entities, boolean allColumnsStatement) {
		
	}
	
	@Override
	public void onError(Iterable<T> entities, RuntimeException runtimeException) {
		
	}
}
