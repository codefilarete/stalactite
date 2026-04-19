package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.model.AncestorJoin;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.ExtraTableJoin;
import org.codefilarete.stalactite.engine.configurer.model.Mapping;
import org.codefilarete.stalactite.engine.configurer.model.PropertyMappingHolder;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Creates and fulfills an {@link Entity} instance representing the root of an aggregate.
 * The result might be consumed by a {@link AggregateResolver} to create a persister instance afterwards.
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
	
	<C, I> Entity<C, I, ?> resolveEntityHierarchy(EntityMappingConfiguration<C, I> rootConfiguration) {
		InheritanceMappingResolver<C, I> inheritanceMappingResolver = new InheritanceMappingResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations = inheritanceMappingResolver.resolveConfigurations(rootConfiguration);
		
		KeyMappingApplier<C, I> keyMappingApplier = new KeyMappingApplier<>(dialect, connectionConfiguration);
		keyMappingApplier.resolve(bottomToTopConfigurations);
		
		Entity<C, I, ?> entity = buildHierarchy(bottomToTopConfigurations);
		
		ResolvedConfiguration<C, I> resolvedRootConfiguration = (ResolvedConfiguration<C, I>) first(bottomToTopConfigurations);
		PolymorphismPolicy<C> polymorphismPolicy = rootConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismMetadataResolver polymorphismMetadataResolver = new PolymorphismMetadataResolver(dialect);
			EntityPolymorphism<C, I> entityPolymorphism = polymorphismMetadataResolver.resolve(resolvedRootConfiguration, polymorphismPolicy);
			entity.setPolymorphism(entityPolymorphism);
		}
		
		return entity;
	}
	
	private <C, I, X, TT extends Table<TT>> Entity<C, I, ?> buildHierarchy(KeepOrderSet<ResolvedConfiguration<?, I>> bottomToTopConfigurations) {
		// Handling very first entity as a seed for eventual next iterations on the hierarchy
		ResolvedConfiguration<C, I> bottomestConfiguration = (ResolvedConfiguration<C, I>) first(bottomToTopConfigurations);
		Table<?> bottomTable = bottomestConfiguration.getTable();
		Entity<C, I, ?> bottomestEntity = this.<C, I, Table>buildEntity(bottomestConfiguration, bottomTable);
		
		final Holder<TT> previousTable = new Holder<>((TT) bottomTable);
		final Holder<Entity<X, I, TT>> previousEntity = new Holder<>((Entity<X, I, TT>) bottomestEntity);
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
	
	private <X, I, T extends Table<T>> Entity<X, I, T> buildEntity(ResolvedConfiguration<X, I> configuration, T table) {
		Entity<X, I, T> result = new Entity<>(configuration.getIdentifierMapping(), new Mapping<>(configuration.getMappingConfiguration().getEntityType(), table));
		addMapping(result, configuration, table);
		return result;
	}
	
	private <X, I, T extends Table<T>, EXTRATABLE extends Table<EXTRATABLE>> void addMapping(Entity<X, I, T> entity, ResolvedConfiguration<X, I> configuration, T table) {
		PropertyMappingResolver<X, T> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		Set<AbstractPropertyMapping<X, ?, T>> mapping = propertyMappingResolver.resolve(configuration.getMappingConfiguration().getPropertiesMapping(), table, configuration.getNamingConfiguration().getColumnNamingStrategy());
		
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
}
