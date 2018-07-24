package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateListenerCollection<E> implements IUpdateListener<E> {
	
	private List<IUpdateListener<E>> updateListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdate(Iterable<UpdatePayload<E, ?>> updatePayloads, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.beforeUpdate(updatePayloads, allColumnsStatement));
	}
	
	@Override
	public void afterUpdate(Iterable<UpdatePayload<E, ?>> entities, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.afterUpdate(entities, allColumnsStatement));
	}
	
	@Override
	public void onError(Iterable<E> entities, RuntimeException runtimeException) {
		updateListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(IUpdateListener<E> updateListener) {
		if (updateListener != null) {    // prevent null as specified in interface
			this.updateListeners.add(updateListener);
		}
	}
}
