package org.codefilarete.stalactite.engine.runtime;

/**
 * @author Guillaume Mary
 */
public interface EntityConfiguredPersister<C, I> extends ConfiguredPersister<C, I> {
	
	@Override
	default I getId(C entity) {
		return getMapping().getId(entity);
	}
}
