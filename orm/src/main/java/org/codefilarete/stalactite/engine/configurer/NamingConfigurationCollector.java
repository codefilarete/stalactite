package org.codefilarete.stalactite.engine.configurer;

import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.engine.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.TableNamingStrategy;
import org.codefilarete.tool.function.Hanger.Holder;

/**
 * Build a {@link NamingConfiguration} from an {@link EntityMappingConfiguration} by iterating over its hierarchy
 * 
 * @author Guillaume Mary
 */
public class NamingConfigurationCollector {
	
	private final EntityMappingConfiguration<?, ?> entityMappingConfiguration;
	
	public NamingConfigurationCollector(EntityMappingConfiguration<?, ?> entityMappingConfiguration) {
		this.entityMappingConfiguration = entityMappingConfiguration;
	}
	
	NamingConfiguration collect() {
		
		org.codefilarete.tool.Nullable<TableNamingStrategy> optionalTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getTableNamingStrategy() != null && !optionalTableNamingStrategy.isPresent()) {
				optionalTableNamingStrategy.set(configuration.getTableNamingStrategy());
			}
		});
		TableNamingStrategy tableNamingStrategy = optionalTableNamingStrategy.getOr(TableNamingStrategy.DEFAULT);
		
		// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too
		org.codefilarete.tool.Nullable<ColumnNamingStrategy> optionalColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		class ColumnNamingStrategyCollector implements Consumer<EmbeddableMappingConfiguration> {
			@Override
			public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
				if (embeddableMappingConfiguration.getColumnNamingStrategy() != null && !optionalColumnNamingStrategy.isPresent()) {
					optionalColumnNamingStrategy.set(embeddableMappingConfiguration.getColumnNamingStrategy());
				}
			}
		}
		ColumnNamingStrategyCollector columnNamingStrategyCollector = new ColumnNamingStrategyCollector();
		visitInheritedEmbeddableMappingConfigurations(entityConfigurationConsumer ->
				columnNamingStrategyCollector.accept(entityConfigurationConsumer.getPropertiesMapping()), columnNamingStrategyCollector);
		ColumnNamingStrategy columnNamingStrategy = optionalColumnNamingStrategy.getOr(ColumnNamingStrategy.DEFAULT);
		
		org.codefilarete.tool.Nullable<ForeignKeyNamingStrategy> optionalForeignKeyNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getForeignKeyNamingStrategy() != null && !optionalForeignKeyNamingStrategy.isPresent()) {
				optionalForeignKeyNamingStrategy.set(configuration.getForeignKeyNamingStrategy());
			}
		});
		ForeignKeyNamingStrategy foreignKeyNamingStrategy = optionalForeignKeyNamingStrategy.getOr(ForeignKeyNamingStrategy.DEFAULT);
		
		org.codefilarete.tool.Nullable<JoinColumnNamingStrategy> optionalJoinColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getJoinColumnNamingStrategy() != null && !optionalJoinColumnNamingStrategy.isPresent()) {
				optionalJoinColumnNamingStrategy.set(configuration.getJoinColumnNamingStrategy());
			}
		});
		JoinColumnNamingStrategy joinColumnNamingStrategy = optionalJoinColumnNamingStrategy.getOr(JoinColumnNamingStrategy.JOIN_DEFAULT);
		
		org.codefilarete.tool.Nullable<ColumnNamingStrategy> optionalIndexColumnNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getIndexColumnNamingStrategy() != null && !optionalIndexColumnNamingStrategy.isPresent()) {
				optionalIndexColumnNamingStrategy.set(configuration.getIndexColumnNamingStrategy());
			}
		});
		ColumnNamingStrategy indexColumnNamingStrategy = optionalIndexColumnNamingStrategy.getOr(ColumnNamingStrategy.INDEX_DEFAULT);
		
		org.codefilarete.tool.Nullable<AssociationTableNamingStrategy> optionalAssociationTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getAssociationTableNamingStrategy() != null && !optionalAssociationTableNamingStrategy.isPresent()) {
				optionalAssociationTableNamingStrategy.set(configuration.getAssociationTableNamingStrategy());
			}
		});
		AssociationTableNamingStrategy associationTableNamingStrategy = optionalAssociationTableNamingStrategy.getOr(AssociationTableNamingStrategy.DEFAULT);
		
		org.codefilarete.tool.Nullable<ElementCollectionTableNamingStrategy> optionalElementCollectionTableNamingStrategy = org.codefilarete.tool.Nullable.empty();
		visitInheritedEntityMappingConfigurations(configuration -> {
			if (configuration.getElementCollectionTableNamingStrategy() != null && !optionalElementCollectionTableNamingStrategy.isPresent()) {
				optionalElementCollectionTableNamingStrategy.set(configuration.getElementCollectionTableNamingStrategy());
			}
		});
		ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy = optionalElementCollectionTableNamingStrategy.getOr(ElementCollectionTableNamingStrategy.DEFAULT);
		
		return new NamingConfiguration(
				tableNamingStrategy,
				columnNamingStrategy,
				foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy,
				joinColumnNamingStrategy,
				indexColumnNamingStrategy,
				associationTableNamingStrategy);
	}
	
	void visitInheritedEntityMappingConfigurations(Consumer<EntityMappingConfiguration> configurationConsumer) {
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(configurationConsumer);
	}
	
	/**
	 * Visits parent {@link EntityMappingConfiguration} of current entity mapping configuration (including itself), this is an optional operation
	 * because current configuration may not have a direct entity ancestor.
	 * Then visits mapped super classes as {@link EmbeddableMappingConfiguration} of the last visited {@link EntityMappingConfiguration}, optional
	 * operation too.
	 * This is because inheritance can only have 2 paths :
	 * - first an optional inheritance from some other entity
	 * - then an optional inheritance from some mapped super class
	 *
	 * @param entityConfigurationConsumer
	 * @param mappedSuperClassConfigurationConsumer
	 */
	void visitInheritedEmbeddableMappingConfigurations(Consumer<EntityMappingConfiguration> entityConfigurationConsumer,
													   Consumer<EmbeddableMappingConfiguration> mappedSuperClassConfigurationConsumer) {
		// iterating over mapping from inheritance
		Holder<EntityMappingConfiguration> lastMapping = new Holder<>();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			entityConfigurationConsumer.accept(entityMappingConfiguration);
			lastMapping.set(entityMappingConfiguration);
		});
		if (lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			// iterating over mapping from mapped super classes
			lastMapping.get().getPropertiesMapping().getMappedSuperClassConfiguration().inheritanceIterable().forEach(mappedSuperClassConfigurationConsumer);
		}
	}
}
