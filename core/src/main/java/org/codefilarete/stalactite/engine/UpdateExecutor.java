package org.codefilarete.stalactite.engine;

import java.util.Collections;

import org.codefilarete.tool.Duo;

/**
 * @author Guillaume Mary
 */
public interface UpdateExecutor<C> {
	
	/**
	 * Updates roughly given entity: no differences are computed, only update statements (full column) are applied.
	 *
	 * @param entity an entity
	 */
	default void updateById(C entity) {
		updateById(Collections.singletonList(entity));
	}
	
	/**
	 * Updates roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * Hence optimistic lock (versioned entities) is not checked.
	 *
	 * @param entities iterable of entities
	 * @apiNote used internally by
	 * {@link org.codefilarete.stalactite.engine.runtime.CollectionUpdater#onRemovedTarget(org.codefilarete.stalactite.engine.runtime.CollectionUpdater.UpdateContext, org.codefilarete.stalactite.engine.diff.AbstractDiff)}
	 */
	void updateById(Iterable<C> entities);
	
	void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement);
}
