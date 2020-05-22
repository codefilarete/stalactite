package org.gama.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface IEntityConfiguredPersister<C, I> extends IEntityPersister<C, I>, IConfiguredPersister<C, I> {
	
	@Override
	default I getId(C entity) {
		return getMappingStrategy().getId(entity);
	}
}
