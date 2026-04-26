package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.MethodReferences;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.IdentifierMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;

import static org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.KeyMapping;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * A class that collects the configurations inherited by a {@link EntityMappingConfiguration} inheritance to build
 * a {@link ResolvedConfiguration} for each configuration in the hierarchy.
 * The {@link ResolvedConfiguration}s are some ready-to-use instances that contain all the necessary information to build
 * an entity, without the need to go through the whole hierarchy again.
 * 
 * @param <C> the initial configuration entity type
 * @param <I> the configurations identifier type
 * @author Guillaume Mary
 */
public class InheritanceConfigurationResolver<C, I> {
	
	@VisibleForTesting
	static final SerializableTriFunction<FluentEntityMappingBuilder, SerializablePropertyMutator<?, ?>, IdentifierPolicy, FluentEntityMappingBuilderKeyOptions<?, ?>> IDENTIFIER_METHOD_REFERENCE = FluentEntityMappingBuilder::mapKey;
	
	/**
	 * Returns a {@link ResolvedConfiguration} for each configuration in the given configuration hierarchy
	 * 
	 * @param mappingConfiguration the configuration to start from
	 * @return a {@link ResolvedConfiguration} for each configuration in the hierarchy

	 */
	KeepOrderSet<ResolvedConfiguration<?, I>> resolveConfigurations(EntityMappingConfiguration<C, I> mappingConfiguration) {
		NamingConfiguration defaultNamingConfiguration = new NamingConfiguration(
				TableNamingStrategy.DEFAULT,
				ColumnNamingStrategy.DEFAULT,
				ForeignKeyNamingStrategy.DEFAULT,
				UniqueConstraintNamingStrategy.DEFAULT,
				ElementCollectionTableNamingStrategy.DEFAULT,
				MapEntryTableNamingStrategy.DEFAULT,
				JoinColumnNamingStrategy.JOIN_DEFAULT,
				ColumnNamingStrategy.INDEX_DEFAULT,
				AssociationTableNamingStrategy.DEFAULT);
		
		NamingConfiguration inheritedNaming = defaultNamingConfiguration;
		KeyMapping<?, I> inheritedKey;
		EntityMappingConfiguration<?, ?> keyDefiner = null;
		
		List<EntityMappingConfiguration<?, I>> bottomToTopConfigurations = Iterables.asList(mappingConfiguration.inheritanceIterable());
		Iterable<EntityMappingConfiguration<?, I>> topToBottomConfigurations = () -> Iterables.reverseIterator(bottomToTopConfigurations);
		
		// Using the same top-to-bottom loop to determine the naming strategies and the identifier because those
		// data follow the same and usual inheritance principle: the ancestor applies if none is defined locally
		KeepOrderSet<ResolvedConfiguration<?, I>> topToBottomResult = new KeepOrderSet<>();
		for (EntityMappingConfiguration<?, I> node : topToBottomConfigurations) {
			ResolvedConfiguration<?, I> nodeConfiguration = new ResolvedConfiguration<>(node);
			topToBottomResult.add(nodeConfiguration);
			
			// 1. Computing naming conventions: it follows usual inheritance rules, the highest configuration declares default
			// behaviors, which can be overridden by sub-configurations
			NamingConfiguration nodeNamingConventions = new NamingConfiguration(
					nullable(node.getTableNamingStrategy()).getOr(inheritedNaming.getTableNamingStrategy()),
					nullable(node.getColumnNamingStrategy()).getOr(inheritedNaming.getColumnNamingStrategy()),
					nullable(node.getForeignKeyNamingStrategy()).getOr(inheritedNaming.getForeignKeyNamingStrategy()),
					nullable(node.getUniqueConstraintNamingStrategy()).getOr(inheritedNaming.getUniqueConstraintNamingStrategy()),
					nullable(node.getElementCollectionTableNamingStrategy()).getOr(inheritedNaming.getElementCollectionTableNamingStrategy()),
					nullable(node.getEntryMapTableNamingStrategy()).getOr(inheritedNaming.getEntryMapTableNamingStrategy()),
					nullable(node.getJoinColumnNamingStrategy()).getOr(inheritedNaming.getJoinColumnNamingStrategy()),
					nullable(node.getIndexColumnNamingStrategy()).getOr(inheritedNaming.getIndexColumnNamingStrategy()),
					nullable(node.getAssociationTableNamingStrategy()).getOr(inheritedNaming.getAssociationTableNamingStrategy()));
			
			nodeConfiguration.setNamingConfiguration(nodeNamingConventions);
			inheritedNaming = nodeNamingConventions;
			
			// 2. Key mapping: it follows the inheritance rules:
			// - null for ancestors before definer,
			// - null for descendants
			// - thus, can't be overridden by sub-configurations: an exception is thrown where it's redefined
			if (node.getKeyMapping() != null) {
				if (keyDefiner != null) {
					// only one definer is allowed in the full chain
					throw new MappingConfigurationException("Identifier policy is defined twice in the hierarchy : first by "
							+ AccessorDefinition.toString(keyDefiner.getKeyMapping().getAccessor())
							+ ", then by " + AccessorDefinition.toString(node.getKeyMapping().getAccessor()));
				}
				inheritedKey = node.getKeyMapping();
				keyDefiner = node;
				nodeConfiguration.setKeyMapping(inheritedKey);
			}
		}
		
		if (keyDefiner == null) {
			throw newMissingIdentificationException(mappingConfiguration.getEntityType());
		}
		
		Map<EntityMappingConfiguration<?, I>, ResolvedConfiguration<?, I>> configurationPerMapping = Iterables.map(topToBottomResult, ResolvedConfiguration::getMappingConfiguration, () -> new HashMap<>());
		// 3. deducing Tables from configuration hierarchy
		// - it's the opposite of usual inheritance rules: the lowest configuration defines the table, which is shared with ancestors if joining tables is not defined
		// - when joining tables flag is set, a new table is created for parent configuration
		// Note that this step depend on naming strategy for Table creation, hence it must be done after naming strategy resolution
		Table currentSegmentTable = takeConfigurationTableOrCreateOne(mappingConfiguration, configurationPerMapping);
		EntityMappingConfiguration<?, I> pawn = mappingConfiguration;
		while(pawn.getInheritanceConfiguration() != null) {
			configurationPerMapping.get(pawn).setTable(currentSegmentTable);
			if (pawn.getInheritanceConfiguration().isJoiningTables()) {
				EntityMappingConfiguration<?, I> parentMappingConfiguration = pawn.getInheritanceConfiguration().getParentMappingConfiguration();
				Table joiningTable = takeConfigurationTableOrCreateOne(parentMappingConfiguration, configurationPerMapping);
				currentSegmentTable = joiningTable;
			}
			pawn = pawn.getInheritanceConfiguration().getParentMappingConfiguration();
		}
		// handling the latest configuration because algorithm above didn't iterate over it
		configurationPerMapping.get(pawn).setTable(currentSegmentTable);
		
		List<ResolvedConfiguration<?, I>> result = new ArrayList<>(topToBottomResult.getDelegate());
		Collections.reverse(result);
		return new KeepOrderSet<>(result);
	}
	
	protected UnsupportedOperationException newMissingIdentificationException(Class<C> entityType) {
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityType)
				+ ", please add one through " + MethodReferences.toMethodReferenceString(IDENTIFIER_METHOD_REFERENCE) + " variants");
	}
	
	@Nullable
	private <X, Y> Table takeConfigurationTableOrCreateOne(EntityMappingConfiguration<X, Y> mappingConfiguration, Map<EntityMappingConfiguration<?, Y>, ResolvedConfiguration<?, Y>> result) {
		return nullable(mappingConfiguration.getTable())
				.elseSet(() -> new Table(result.get(mappingConfiguration).getNamingConfiguration()
						.getTableNamingStrategy()
						.giveName(mappingConfiguration.getEntityType())))
				.get();
	}
	
	static class ResolvedConfiguration<C, I> {
		
		private final EntityMappingConfiguration<C, I> mappingConfiguration;
		
		private Table table;
		
		private NamingConfiguration namingConfiguration;
		
		/**
		 * Null for ancestors before definer, inherited for descendants
		 */
		@Nullable
		private KeyMapping<C, I> keyMapping;
		
		@Nullable
		private IdentifierMapping<C, I> identifierMapping;
		
		ResolvedConfiguration(EntityMappingConfiguration<C, I> node) {
			this.mappingConfiguration = node;
		}
		
		public EntityMappingConfiguration<C, I> getMappingConfiguration() {
			return mappingConfiguration;
		}
		
		public Table getTable() {
			return table;
		}
		
		public void setTable(Table table) {
			this.table = table;
		}
		
		public NamingConfiguration getNamingConfiguration() {
			return namingConfiguration;
		}
		
		public void setNamingConfiguration(NamingConfiguration namingConfiguration) {
			this.namingConfiguration = namingConfiguration;
		}
		
		@Nullable
		public KeyMapping getKeyMapping() {
			return keyMapping;
		}
		
		public void setKeyMapping(@Nullable KeyMapping keyMapping) {
			this.keyMapping = keyMapping;
		}
		
		@Nullable
		public IdentifierMapping getIdentifierMapping() {
			return identifierMapping;
		}
		
		public void setIdentifierMapping(@Nullable IdentifierMapping identifierMapping) {
			this.identifierMapping = identifierMapping;
		}
	}
}
