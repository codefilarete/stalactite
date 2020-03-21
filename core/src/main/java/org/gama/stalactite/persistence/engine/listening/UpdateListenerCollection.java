package org.gama.stalactite.persistence.engine.listening;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.Duo;

/**
 * @author Guillaume Mary
 */
public class UpdateListenerCollection<E> implements UpdateListener<E> {
	
	private List<UpdateListener<E>> updateListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdate(Iterable<? extends Duo<? extends E, ? extends E>> updatePayloads, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.beforeUpdate(updatePayloads, allColumnsStatement));
	}
	
	@Override
	public void afterUpdate(Iterable<? extends Duo<? extends E, ? extends E>> entities, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.afterUpdate(entities, allColumnsStatement));
	}
	
	@Override
	public void onError(Iterable<? extends E> entities, RuntimeException runtimeException) {
		updateListeners.forEach(listener -> listener.onError(entities, runtimeException));
	}
	
	public void add(UpdateListener<E> updateListener) {
		this.updateListeners.add(updateListener);
	}
}
