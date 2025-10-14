package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.tool.Reflections;

/**
 * Dedicated {@link BuildLifeCycleListener} that will consume the persister found in the registry.
 * Used in particular to deal with bean cycle load.
 *
 * @param <P> entity type to be persisted
 * @see #consume(ConfiguredRelationalPersister)
 */
public abstract class PostInitializer<P> implements BuildLifeCycleListener {
	
	/**
	 * Entity type of persister to be post initialized
	 */
	private final Class<P> entityType;
	
	protected PostInitializer(Class<P> entityType) {
		this.entityType = entityType;
	}
	
	public Class<P> getEntityType() {
		return entityType;
	}
	
	@Override
	public final void afterBuild() {
		try {
			consume((ConfiguredRelationalPersister<P, ?>) PersisterBuilderContext.CURRENT.get().getPersisterRegistry().getPersister(entityType));
		} catch (RuntimeException e) {
			throw new MappingConfigurationException("Error while post processing persister of type "
					+ Reflections.toString(entityType), e);
		}
	}
	
	@Override
	public final void afterAllBuild() {
		// nothing special to do here
	}
	
	/**
	 * Invoked after main entity graph creation
	 *
	 * @param persister entity type persister
	 */
	public abstract void consume(ConfiguredRelationalPersister<P, ?> persister);
}
