package org.codefilarete.stalactite.engine.listener;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.tool.Duo;

/**
 * @author Guillaume Mary
 */
public class UpdateListenerCollection<E> implements UpdateListener<E> {
	
	private List<UpdateListener<E>> updateListeners = new ArrayList<>();
	
	@Override
	public void beforeUpdate(Iterable<? extends Duo<E, E>> updatePayloads, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.beforeUpdate(updatePayloads, allColumnsStatement));
	}
	
	@Override
	public void afterUpdate(Iterable<? extends Duo<E, E>> entities, boolean allColumnsStatement) {
		updateListeners.forEach(listener -> listener.afterUpdate(entities, allColumnsStatement));
	}
	
	@Override
	public void onUpdateError(Iterable<? extends E> entities, RuntimeException runtimeException) {
		updateListeners.forEach(listener -> listener.onUpdateError(entities, runtimeException));
	}
	
	public void add(UpdateListener<E> updateListener) {
		this.updateListeners.add(updateListener);
	}
	
	/**
	 * Move internal listeners to given instance.
	 * Useful to aggregate listeners into a single instance.
	 * Please note that as this method is named "move" it means that listeners of current instance will be cleared.
	 *
	 * @param updateListener the target listener on which the one of current instance must be moved to.
	 */
	public void moveTo(UpdateListenerCollection<E> updateListener) {
		updateListener.updateListeners.addAll(this.updateListeners);
		this.updateListeners.clear();
	}
	
}
