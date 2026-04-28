package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.resolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Creates and fulfills an {@link Entity} instance representing the root of an aggregate.
 * The result might be consumed by a {@link AggregateResolver} to create a persister instance afterward.
 * 
 * @author Guillaume Mary
 */
public class AggregateMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public AggregateMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	<C, I> Entity<C, I, ?> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		InheritanceConfigurationResolver<C, I> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations = inheritanceConfigurationResolver.resolveConfigurations(rootConfiguration);
		
		InheritanceMetadataResolver<C, I, ?> keyMappingApplier = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		EntitySource<C, I> entityHierarchy = keyMappingApplier.resolve(bottomToTopConfigurations);
		Entity<C, I, ?> firstEntity = entityHierarchy.getEntity();
		
		RelationsMetadataResolver relationsMetadataResolver = new RelationsMetadataResolver(dialect, connectionConfiguration);
		relationsMetadataResolver.resolve(entityHierarchy);
		
		ResolvedConfiguration<C, I> resolvedRootConfiguration = (ResolvedConfiguration<C, I>) first(bottomToTopConfigurations);
		PolymorphismPolicy<C> polymorphismPolicy = rootConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismMetadataResolver polymorphismMetadataResolver = new PolymorphismMetadataResolver(dialect);
			EntityPolymorphism<C, I> entityPolymorphism = polymorphismMetadataResolver.resolve(resolvedRootConfiguration, polymorphismPolicy);
			firstEntity.setPolymorphism(entityPolymorphism);
		}
		
		return firstEntity;
	}
}
