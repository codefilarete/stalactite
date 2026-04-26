package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.Mapping;
import org.codefilarete.stalactite.engine.configurer.model.PropertyMappingHolder;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Look up an {@link EntityMappingConfiguration} inheritance hierarchy and creates an {@link Entity} instance from it
 * with the following information set:
 * - identifier mapping
 * - property-to-column bindings
 * - joins for extra tables (secondary tables)
 * - all ancestors
 *
 * This is the first step of creating a fully configured {@link Entity} instance. The next ones are:
 * - relation
 * - polymorphism
 */
public class InheritanceMetadataResolver<C, I, T extends Table<T>> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public InheritanceMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	/**
	 * Resolves the inheritance hierarchy of the given entity configuration into a ready-to-use
	 * {@link Entity} graph.
	 *
	 * @param entityConfiguration the configuration to transform into an {@link Entity} instance
	 * @return an {@link Entity} instance, filled with identifier and ancestors information
	 */
	Entity<C, I, T> resolve(EntityMappingConfiguration<C, I> entityConfiguration) {
		InheritanceConfigurationResolver<C, I> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations = inheritanceConfigurationResolver.resolveConfigurations(entityConfiguration);
		return resolve(bottomToTopConfigurations);
	}
	
	/**
	 * Resolves the inheritance hierarchy of the given entity configuration into a ready-to-use
	 * {@link Entity} graph.
	 *
	 * @param bottomToTopConfigurations the whole configuration hierarchy, ordered from bottom to top, the first one will be transformed into an {@link Entity} instance 
	 * @return an {@link Entity} instance, filled with identifier and ancestors information
	 */
	Entity<C, I, T> resolve(KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations) {
		KeyMappingApplier<C, I> keyMappingApplier = new KeyMappingApplier<>(dialect, connectionConfiguration);
		keyMappingApplier.resolve(bottomToTopConfigurations);
		
		return buildHierarchy(bottomToTopConfigurations);
	}
	
	private <X, TT extends Table<TT>> Entity<C, I, T> buildHierarchy(KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations) {
		// Handling very first entity as a seed for eventual next iterations on the hierarchy
		ResolvedConfiguration<C, I> bottomestConfiguration = (ResolvedConfiguration<C, I>) first(bottomToTopConfigurations);
		T bottomTable = (T) bottomestConfiguration.getTable();
		Entity<C, I, T> bottomestEntity = this.buildEntity(bottomestConfiguration, bottomTable);
		
		final Hanger.Holder<TT> previousTable = new Hanger.Holder<>((TT) bottomTable);
		final Hanger.Holder<Entity<X, I, TT>> previousEntity = new Hanger.Holder<>((Entity<X, I, TT>) bottomestEntity);
		bottomToTopConfigurations.stream().skip(1)
				.map(((Class<ResolvedConfiguration<X, I>>) (Class) ResolvedConfiguration.class)::cast)
				.forEach(resolvedConfigurationPawn -> {
					TT resolvedTable = (TT) resolvedConfigurationPawn.getTable();
					if (previousTable.get() != resolvedTable) {
						Entity<X, I, TT> ancestor = buildEntity(resolvedConfigurationPawn, resolvedTable);
						previousEntity.get().setParent(new AncestorJoin<>(ancestor, new DirectRelationJoin<>(previousTable.get().getPrimaryKey(), resolvedTable.getPrimaryKey())));
						// preparing next iteration
						previousTable.set(resolvedTable);
						previousEntity.set(ancestor);
					} else {
						addMapping(previousEntity.get(), resolvedConfigurationPawn, previousTable.get());
					}
				});
		return bottomestEntity;
	}
	
	private <X, TT extends Table<TT>> Entity<X, I, TT> buildEntity(ResolvedConfiguration<X, I> configuration, TT table) {
		Entity<X, I, TT> result = new Entity<>(configuration.getIdentifierMapping(), new Mapping<>(configuration.getMappingConfiguration().getEntityType(), table));
		addMapping(result, configuration, table);
		return result;
	}
	
	private <X, TT extends Table<TT>, EXTRATABLE extends Table<EXTRATABLE>> void addMapping(Entity<X, I, TT> entity,
	                                                                                         ResolvedConfiguration<X, I> configuration,
	                                                                                         TT table) {
		PropertyMappingResolver<X, TT> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		Set<AbstractPropertyMapping<X, ?, TT>> mapping = propertyMappingResolver.resolve(
				configuration.getMappingConfiguration().getPropertiesMapping(),
				table,
				configuration.getNamingConfiguration().getColumnNamingStrategy());
		
		Set<AbstractPropertyMapping<X, ?, EXTRATABLE>> extraTableMappings = new KeepOrderSet<>();
		mapping.forEach(mappingPawn -> {
			if (mappingPawn.getColumn().getTable() == table) {
				entity.getPropertyMappingHolder().addMapping(mappingPawn);
			} else {
				extraTableMappings.add((AbstractPropertyMapping<X, ?, EXTRATABLE>) mappingPawn);
			}
		});
		addExtraTableProperties(entity, extraTableMappings, configuration.getNamingConfiguration().getForeignKeyNamingStrategy());
		
	}
	
	private <X, TT extends Table<TT>, EXTRATABLE extends Table<EXTRATABLE>> void addExtraTableProperties(Entity<X, I, TT> result,
	                                                                                                     Collection<AbstractPropertyMapping<X, ?, EXTRATABLE>> extraTableProperties,
	                                                                                                     ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Map<EXTRATABLE, KeepOrderSet<AbstractPropertyMapping<X, ?, EXTRATABLE>>> propertiesPerTable = new KeepOrderMap<>();
		extraTableProperties.forEach(mapping -> {
			KeepOrderSet<AbstractPropertyMapping<X, ?, EXTRATABLE>> mappings = propertiesPerTable.computeIfAbsent(mapping.getColumn().getTable(), k -> new KeepOrderSet<>());
			mappings.add(mapping);
		});
		propertiesPerTable.forEach((extraTable, mapping) -> {
			if (extraTable.getPrimaryKey() == null) {
				PrimaryKeyPropagator<TT, EXTRATABLE, I> primaryKeyPropagator = new PrimaryKeyPropagator<>();
				primaryKeyPropagator.propagate(result.getTable().getPrimaryKey(), extraTable, foreignKeyNamingStrategy);
			}
			PropertyMappingHolder<X, EXTRATABLE> mappingHolder = new PropertyMappingHolder<>();
			mappingHolder.addMapping(mapping);
			ExtraTableJoin<X, TT, EXTRATABLE, I> relation = new ExtraTableJoin<>(mappingHolder, result.getTable().<I>getPrimaryKey(), extraTable.getPrimaryKey());
			result.addRelation(relation);
		});
	}
}
