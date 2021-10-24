package org.gama.stalactite.persistence.engine;

import java.util.Collections;

import org.gama.lang.Duo;

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
	 * {@link org.gama.stalactite.persistence.engine.runtime.CollectionUpdater#onRemovedTarget(org.gama.stalactite.persistence.engine.runtime.CollectionUpdater.UpdateContext,
	 * org.gama.stalactite.persistence.id.diff.AbstractDiff)}
	 */
	void updateById(Iterable<C> entities);
	
	void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement);
}
