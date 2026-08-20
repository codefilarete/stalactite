package org.codefilarete.stalactite.engine.configurer.dslresolver;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Creates and fulfills an {@link AbstractEntity} instance representing the root of an aggregate.
 * The result might be consumed by a {@link org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver} to create a persister instance afterward.
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
	
	public <C, I> AbstractEntity<C, I, ?> resolve(EntityMappingConfiguration<C, I> rootConfiguration) {
		InheritanceConfigurationResolver<C, I> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations = inheritanceConfigurationResolver.resolveConfigurations(rootConfiguration);
		
		InheritanceMetadataResolver<C, I, ?> keyMappingApplier = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		EntitySource<C, I> entityHierarchy = keyMappingApplier.resolve(bottomToTopConfigurations);
		
		RelationsMetadataResolver relationsMetadataResolver = new RelationsMetadataResolver(dialect, connectionConfiguration);
		relationsMetadataResolver.resolve(entityHierarchy);
		
		return entityHierarchy.getEntity();
	}
}
