package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Guillaume Mary
 */
public class UpdateListenerCollection<E> implements UpdateListener<E> {
	
	private List<UpdateListener<E>> updateListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdate(Iterable<UpdatePayload<? extends E, ?>> updatePayloads, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.beforeUpdate(updatePayloads, allColumnsStatement));
	}
	
	@Override
	public void afterUpdate(Iterable<UpdatePayload<? extends E, ?>> entities, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.afterUpdate(entities, allColumnsStatement));
	}
	
	@Override
	public void onError(Iterable<? extends E> entities, RuntimeException runtimeException) {
		updateListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(UpdateListener<E> updateListener) {
		if (updateListener != null) {    // prevent null as specified in interface
			this.updateListeners.add(updateListener);
		}
	}
}
