package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.persistence.engine.IEntityPersister;

/**
 * @author Guillaume Mary
 */
public interface IEntityConfiguredPersister<C, I> extends IEntityPersister<C, I>, IConfiguredPersister<C, I> {
	
	@Override
	default I getId(C entity) {
		return getMappingStrategy().getId(entity);
	}
}
