package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.function.Function;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.PersisterRegistry.DefaultPersisterRegistry;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.EntityIsManagedByPersisterAsserter;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;

public class DefaultPersisterBuilder {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	
	public DefaultPersisterBuilder(PersistenceContext persistenceContext) {
		this(persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration(), new DefaultPersisterRegistry(persistenceContext.getPersisters()));
	}
	
	/**
	 *
	 * @param dialect the dialect to use to adapt SQL to the database
	 * @param connectionConfiguration connection information to adapt SQL or create Sequence for identifier generation
	 * @param persisterRegistry any existing {@link PersisterRegistry} that can provide any existing {@link org.codefilarete.stalactite.engine.EntityPersister}
	 * 	to be reused
	 */
	public DefaultPersisterBuilder(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.persisterRegistry = persisterRegistry;
	}
	
	/**
	 * Builds a {@link EntityPersister} instance for the entity mapping configuration provided by given argument.
	 * An exception is thrown if an already existing {@link EntityPersister} for the entity type already exists
	 * (the check doesn't account for potential differences in mapping configurations of the entity type).
	 * 
	 * @param <C> the type of the entity being persisted
	 * @param <I> the type of the identifier for the entity
	 * @param entityMappingConfiguration the provider for the entity mapping configuration used to construct the persister
	 * @return the constructed {@link ConfiguredRelationalPersister} instance
	 * @throws IllegalStateException if a persister for the entity type already exists
	 */
	public <C, I> ConfiguredRelationalPersister<C, I, ?> build(EntityMappingConfigurationProvider<C, I> entityMappingConfiguration) {
		return build(entityMappingConfiguration.getConfiguration());
	}
	
	/**
	 * Builds a {@link EntityPersister} instance for the given entity mapping configuration.
	 * An exception is thrown if an already existing {@link EntityPersister} for the entity type already exists
	 * (the check doesn't account for potential differences in mapping configurations of the entity type).
	 * 
	 * @param <C> the type of the entity being persisted
	 * @param <I> the type of the identifier for the entity
	 * @param entityMappingConfiguration the configuration used to map the entity and construct the persister
	 * @return the constructed {@link ConfiguredRelationalPersister} instance for the given configuration
	 * @throws IllegalStateException if a persister for the entity type already exists
	 */
	public <C, I> ConfiguredRelationalPersister<C, I, ?> build(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		// If a persister already exists for the type, then we return it to manage the case of graph that declares twice / several times the same mapped type
		// WARN : this does not take mapping configuration differences into account, so if configuration is different from previous one, since
		// no check is done, then the very first persister is returned
		EntityPersister<C, Object> existingPersister = persisterRegistry.getPersister(entityMappingConfiguration.getEntityType());
		if (existingPersister != null) {
			// we can cast because all persisters we registered implement the interface
			throw new IllegalStateException("Persister already exists for " + Reflections.toString(entityMappingConfiguration.getEntityType()));
		} else {
			return doBuild(entityMappingConfiguration);
		}
	}
	
	/**
	 * Builds a {@link EntityPersister} instance for the given entity mapping configuration or return the existing one if any.
	 * (the check doesn't account for potential differences in mapping configurations of the entity type).
	 * This method is not considered to be part of the public API, because the end user should be warned about duplicate
	 * entity persisters, thus, other official methods {@link #build(EntityMappingConfiguration)} are preferred.
	 * 
	 * @param <C> the type of the entity being persisted
	 * @param <I> the type of the identifier for the entity
	 * @param entityMappingConfiguration the configuration used to map the entity and construct the persister
	 * @return the constructed {@link ConfiguredRelationalEntityPersister} instance for the given configuration
	 */
	public <C, I> ConfiguredRelationalEntityPersister<C, I, ?> buildOrGiveExisting(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		// If a persister already exists for the type, then we return it : case of graph that declares twice / several times same mapped type
		// WARN : this does not take mapping configuration differences into account, so if configuration is different from previous one, since
		// no check is done, then the very first persister is returned
		EntityPersister<C, Object> existingPersister = persisterRegistry.getPersister(entityMappingConfiguration.getEntityType());
		if (existingPersister != null) {
			// we can cast because all persisters we registered implement the interface
			return (ConfiguredRelationalEntityPersister<C, I, ?>) existingPersister;
		} else {
			return doBuild(entityMappingConfiguration);
		}
	}
	
	private <C, I> ConfiguredRelationalEntityPersister<C, I, ?> doBuild(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		ConfiguredRelationalEntityPersister<C, I, ?> result;
		result = decorateWithUpdateOptimization((adaptedConnectionConfiguration) -> {
			PersisterBuilderPipeline<C, I> persisterBuilderPipeline = new PersisterBuilderPipeline<>(dialect, adaptedConnectionConfiguration, persisterRegistry);
			return persisterBuilderPipeline.build(entityMappingConfiguration);
		});
		result = decorateWithEntityManagementAsserter(result);
		return result;
	}
	
	private <C, I, T extends Table<T>> OptimizedUpdatePersister<C, I, T> decorateWithUpdateOptimization(Function<ConnectionConfiguration, ConfiguredRelationalEntityPersister<C, I, T>> builderDelegate) {
		// we wrap final result with some transversal features
		// NB: Order of wrap is important due to invocation of instance methods with code like "this.doSomething(..)" in particular with OptimizedUpdatePersister
		// which internally calls update(C, C, boolean) on update(id, Consumer): the latter method is not listened by EntityIsManagedByPersisterAsserter
		// (because it has no purpose since entity is not given as argument) but update(C, C, boolean) is and should be, that is not the case if
		// EntityIsManagedByPersisterAsserter is done first since OptimizedUpdatePersister invokes itself with "this.update(C, C, boolean)"
		ConfiguredRelationalEntityPersister<C, I, T> concretePersister = builderDelegate.apply(OptimizedUpdatePersister.wrapWithQueryCache(connectionConfiguration));
		return new OptimizedUpdatePersister<>(concretePersister);
	}
	
	private <C, I, T extends Table<T>> ConfiguredRelationalEntityPersister<C, I, T> decorateWithEntityManagementAsserter(ConfiguredRelationalEntityPersister<C, I, T> concretePersister) {
		return new EntityIsManagedByPersisterAsserter<>(concretePersister);
	}
}
