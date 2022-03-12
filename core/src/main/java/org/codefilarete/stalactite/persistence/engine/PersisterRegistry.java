package org.codefilarete.stalactite.persistence.engine;

import org.codefilarete.stalactite.persistence.engine.runtime.Persister;

/**
 * Contract for {@link Persister} registry.
 * Made to avoid passing {@link PersistenceContext} to {@link org.codefilarete.stalactite.persistence.engine.configurer.PersisterBuilderImpl}
 * 
 * @author Guillaume Mary
 */
public interface PersisterRegistry {
	
	/**
	 * Returns the {@link Persister} mapped for a class.
	 *
	 * @param clazz the class for which the {@link Persister} must be given
	 * @param <C> the type of the persisted entity
	 * @return null if class has no persister registered
	 * @throws IllegalArgumentException if the class is not mapped
	 */
	<C, I> EntityPersister<C, I> getPersister(Class<C> clazz);
	
	/**
	 * Registers a {@link Persister} on this instance. May overwrite an existing one
	 *
	 * @param persister any {@link Persister}
	 * @param <C> type of persisted bean
	 */
	<C> void addPersister(EntityPersister<C, ?> persister);
	
}
