package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.first;

public class KeyMappingApplier<C, I> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public KeyMappingApplier(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	/**
	 * Fills in {@link ResolvedConfiguration#setIdentifierMapping(IdentifierMapping)} on all the given configurations.
	 * This ensures that for each configuration, the identifier mapping is set.
	 * The logic takes the key mapping defined by the highest (and only) identifier policy and applies it to the very
	 * first entity of the path. Then, for each entity on the path, it creates a primary key and a foreign key as well
	 * as a default identifier mapping that takes the initial one as a source.
	 * 
	 * @param bottomToTopConfigurations the configurations to process, ordered from bottom to top
	 */
	void resolve(KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations) {
		boolean firstTableChange = true;
		ResolvedConfiguration<?, I> keyDefiner = null;
		AssignedByAnotherIdentifierMapping<?, I> assignedByAnotherIdentifierMapping;
		Iterable<ResolvedConfiguration<?, I>> topToBottomConfigurations = () -> Iterables.reverseIterator(bottomToTopConfigurations.getDelegate());
		ResolvedConfiguration<?, I> previousConfiguration = first(topToBottomConfigurations);
		Table previousTable = previousConfiguration.getTable();
		for (ResolvedConfiguration<?, I> resolvedConfiguration : topToBottomConfigurations) {
			if (resolvedConfiguration.getKeyMapping() != null) {
				// we keep the definer for "later": at time when we detect table change to better match table change logic
				keyDefiner = resolvedConfiguration;
			}
			if (previousTable != resolvedConfiguration.getTable()) {
				if (firstTableChange) {
					// very first "entity" on the path, so it takes the identifier manager
					PrimaryKeyResolver<C, I> keyStep = new PrimaryKeyResolver<>();
					keyStep.addIdentifyingPrimarykey(keyDefiner.getKeyMapping(),
							// Note that primary key must be created on previous table
							previousTable,
							dialect.getColumnBinderRegistry(),
							resolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy(),
							resolvedConfiguration.getNamingConfiguration().getUniqueConstraintNamingStrategy());
					// we have a table change, so we need to propagate the primary key
					PrimaryKeyPropagator primaryKeyPropagator = new PrimaryKeyPropagator<>();
					primaryKeyPropagator.propagate(previousTable.<I>getPrimaryKey(),
							resolvedConfiguration.getTable(),
							resolvedConfiguration.getNamingConfiguration().getForeignKeyNamingStrategy());
					
					// setting identifier mapping, this must be done after primary key creation because some
					// identifier managers require them to be set
					IdentifierMappingBuilder<?, I> identifierMappingBuilder = new IdentifierMappingBuilder<>(keyDefiner.getMappingConfiguration(), keyDefiner, dialect, connectionConfiguration);
					IdentifierMapping<?, I> identifierMapping = identifierMappingBuilder.build();
					previousConfiguration.setIdentifierMapping(identifierMapping);
					assignedByAnotherIdentifierMapping = new AssignedByAnotherIdentifierMapping(identifierMapping);
					resolvedConfiguration.setIdentifierMapping(assignedByAnotherIdentifierMapping);
					firstTableChange = false;
				}
			}
			previousTable = resolvedConfiguration.getTable();
			previousConfiguration = resolvedConfiguration;
		}
		// algorithm above doesn't take into account the straight inheritance without table change, here's below
		// what fixes it.
		ResolvedConfiguration<?, I> bottomResolvedConfiguration = first(bottomToTopConfigurations);
		if (firstTableChange) {
			// very first "entity" on the path, so it takes the identifier manager
			PrimaryKeyResolver<C, I> keyStep = new PrimaryKeyResolver<>();
			keyStep.addIdentifyingPrimarykey(keyDefiner.getKeyMapping(),
					bottomResolvedConfiguration.getTable(),
					dialect.getColumnBinderRegistry(),
					bottomResolvedConfiguration.getNamingConfiguration().getColumnNamingStrategy(),
					bottomResolvedConfiguration.getNamingConfiguration().getUniqueConstraintNamingStrategy());
			IdentifierMappingBuilder<?, I> identifierMappingBuilder = new IdentifierMappingBuilder<>(
					keyDefiner.getMappingConfiguration(),
					bottomResolvedConfiguration,
					dialect,
					connectionConfiguration);
			IdentifierMapping<?, I> identifierMapping = identifierMappingBuilder.build();
			bottomResolvedConfiguration.setIdentifierMapping(identifierMapping);
		}
	}
}
