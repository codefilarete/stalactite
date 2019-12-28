package org.gama.stalactite.persistence.engine;

import org.gama.lang.Duo;

/**
 * @author Guillaume Mary
 */
public interface IUpdateExecutor<C> {
	
	/**
	 * Updates roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * Hence optimistic lock (versioned entities) is not checked
	 *
	 * @param entities iterable of entities
	 */
	int updateById(Iterable<C> entities);
	
	int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement);
}
