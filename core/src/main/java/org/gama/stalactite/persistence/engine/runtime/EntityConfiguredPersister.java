package org.codefilarete.stalactite.persistence.engine.runtime;

import org.codefilarete.stalactite.persistence.engine.EntityPersister;

/**
 * @author Guillaume Mary
 */
public interface EntityConfiguredPersister<C, I> extends EntityPersister<C, I>, ConfiguredPersister<C, I> {
	
	@Override
	default I getId(C entity) {
		return getMappingStrategy().getId(entity);
	}
}
