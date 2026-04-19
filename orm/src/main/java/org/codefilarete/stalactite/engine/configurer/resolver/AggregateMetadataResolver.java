package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Collection;
import java.util.Map;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.NamingConfigurationCollector;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.engine.configurer.model.PropertyMappingHolder;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.resolver.PropertyMappingResolver.ResolvedPropertyMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.tool.collection.Iterables.first;

public class AggregateMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public AggregateMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	<C, I> Entity<C, I, ?> resolveEntityHierarchy(EntityMappingConfiguration<C, I> bottomestConfiguration) {
		InheritanceMappingResolver<C, I> inheritanceMappingResolver = new InheritanceMappingResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations = inheritanceMappingResolver.resolveConfigurations(bottomestConfiguration);
		
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
		
		Entity<C, I, ?> entity = buildEntities(bottomToTopConfigurations);
		
		return entity;
	}
	
	private <C, I, X, TT extends Table<TT>, EXTRATABLE extends Table<EXTRATABLE>> Entity<C, I, ?> buildEntities(KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations) {
		// Handling very first entity as a seed for eventual next iterations on the hierarchy
		ResolvedConfiguration<C, I> bottomestConfiguration = (ResolvedConfiguration<C, I>) first(bottomToTopConfigurations);
		Table<?> bottomTable = bottomestConfiguration.getTable();
		Entity<C, I, ?> bottomestEntity = this.<C, I, Table, EXTRATABLE>buildEntity(bottomestConfiguration, bottomTable);
		
		final Holder<TT> previousTable = new Holder<>((TT) bottomTable);
		final Holder<Entity<X, I, TT>> previousEntity = new Holder<>((Entity<X, I, TT>) bottomestEntity);
		bottomToTopConfigurations.stream().skip(1)
				.map(((Class<ResolvedConfiguration<X, I>>) (Class) ResolvedConfiguration.class)::cast)
				.forEach(resolvedConfigurationPawn -> {
			EntityMappingConfiguration<X, I> mappingConfiguration = resolvedConfigurationPawn.getMappingConfiguration();
			TT resolvedTable = (TT) resolvedConfigurationPawn.getTable();
			if (previousTable.get() != resolvedTable) {
				Entity<X, I, TT> ancestor = buildEntity(resolvedConfigurationPawn, resolvedTable);
				previousEntity.get().setParent(new AncestorJoin<>(ancestor, new DirectRelationJoin<>(previousTable.get().getPrimaryKey(), resolvedTable.getPrimaryKey())));
				// preparing next iteration
				previousTable.set(resolvedTable);
				previousEntity.set(ancestor);
			} else {
				ResolvedPropertyMapping<X, TT> propertiesMapping = collectDirectMapping(mappingConfiguration, previousTable.get());
				previousEntity.get().getPropertyMappingHolder().addMapping(propertiesMapping.getMappings());
				addExtraTableProperties(previousEntity.get(), propertiesMapping.<EXTRATABLE>getExtraTableMappings(), resolvedConfigurationPawn.getNamingConfiguration().getForeignKeyNamingStrategy());
			}
		});
		return bottomestEntity;
	}	
	
	private <X, I, T extends Table<T>, EXTRATABLE extends Table<EXTRATABLE>> Entity<X, I, T> buildEntity(ResolvedConfiguration<X, I> configuration, T table) {
		Entity<X, I, T> result = new Entity<>(configuration.getMappingConfiguration().getEntityType(), table, configuration.getIdentifierMapping());
		ResolvedPropertyMapping<X, T> propertiesMapping = collectDirectMapping(configuration.getMappingConfiguration(), table);
		result.getPropertyMappingHolder().addMapping(propertiesMapping.getMappings());
		addExtraTableProperties(result, propertiesMapping.<EXTRATABLE>getExtraTableMappings(), configuration.getNamingConfiguration().getForeignKeyNamingStrategy());
		return result;
	}
	
	private <X, I, T extends Table<T>, EXTRATABLE extends Table<EXTRATABLE>> void addExtraTableProperties(Entity<X, I, T> result, Collection<AbstractPropertyMapping<X, ?, EXTRATABLE>> extraTableProperties, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Map<EXTRATABLE, KeepOrderSet<AbstractPropertyMapping<X, ?, EXTRATABLE>>> propertiesPerTable = new KeepOrderMap<>();
		extraTableProperties.forEach(mapping -> {
			KeepOrderSet<AbstractPropertyMapping<X, ?, EXTRATABLE>> mappings = propertiesPerTable.computeIfAbsent(mapping.getColumn().getTable(), k -> new KeepOrderSet<>());
			mappings.add(mapping);
		});
		propertiesPerTable.forEach((extraTable, mapping) -> {
			if (extraTable.getPrimaryKey() == null) {
				PrimaryKeyPropagator<T, EXTRATABLE, I> primaryKeyPropagator = new PrimaryKeyPropagator<>();
				primaryKeyPropagator.propagate(result.getTable().getPrimaryKey(),
						extraTable,
						foreignKeyNamingStrategy);
			}
			PropertyMappingHolder<X, EXTRATABLE> mappingHolder = new PropertyMappingHolder<>();
			mappingHolder.addMapping(mapping);
			ExtraTableJoin<X, T, EXTRATABLE, I> relation = new ExtraTableJoin<>(mappingHolder, result.getTable().<I>getPrimaryKey(), extraTable.getPrimaryKey());
			result.addRelation(relation);
		});
	}
	
	/**
	 * Collect the mapping from the given configuration
	 * - without checking inheritance hierarchy,
	 * - but looking at embedded configurations
	 * - without looking at extra-table properties.
	 *
	 * The goal is to collect the properties that will go to the given table.
	 *
	 * @param configuration the configuration to collect the properties from
	 * @param table the target table for persistence
	 * @return the set of properties that will be persisted in the given table
	 * @param <C> the type of the configuration
	 */
	private <C, T extends Table<T>> ResolvedPropertyMapping<C, T> collectDirectMapping(EntityMappingConfiguration<C, ?> configuration, T table) {
		NamingConfigurationCollector localNamingConfigurationCollector = new NamingConfigurationCollector(configuration);
		NamingConfiguration localNamingConfiguration = localNamingConfigurationCollector.collect();
		PropertyMappingResolver<C, T> propertyMappingResolver = new PropertyMappingResolver<>(configuration.getPropertiesMapping(), table, dialect.getColumnBinderRegistry(), localNamingConfiguration.getColumnNamingStrategy());
		return propertyMappingResolver.build();
	}
	
}
