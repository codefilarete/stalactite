package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.runtime.BeanPersister;

/**
 * Contract for {@link BeanPersister} registry.
 * Made to avoid passing {@link PersistenceContext} to {@link org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl}
 * 
 * @author Guillaume Mary
 */
public interface PersisterRegistry {
	
	/**
	 * Returns the {@link BeanPersister} mapped for a class.
	 *
	 * @param clazz the class for which the {@link BeanPersister} must be given
	 * @param <C> the type of the persisted entity
	 * @return null if class has no persister registered
	 * @throws IllegalArgumentException if the class is not mapped
	 */
	<C, I> EntityPersister<C, I> getPersister(Class<C> clazz);
	
	/**
	 * Registers a {@link BeanPersister} on this instance. May overwrite an existing one
	 *
	 * @param persister any {@link BeanPersister}
	 * @param <C> type of persisted bean
	 */
	<C> void addPersister(EntityPersister<C, ?> persister);
	
}
