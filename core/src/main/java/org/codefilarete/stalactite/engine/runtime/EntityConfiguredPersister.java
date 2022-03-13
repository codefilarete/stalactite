package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.EntityPersister;

/**
 * @author Guillaume Mary
 */
public interface EntityConfiguredPersister<C, I> extends EntityPersister<C, I>, ConfiguredPersister<C, I> {
	
	@Override
	default I getId(C entity) {
		return getMappingStrategy().getId(entity);
	}
}
