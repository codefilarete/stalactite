package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.InheritanceConfiguration;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Fill a {@link Map} of {@link Table} for each entity configuration found in its inheritance (mapped super classes)
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class TableMappingStep<C, I> {
	
	Map<EntityMappingConfiguration, Table> mapEntityConfigurationToTable(EntityMappingConfiguration<C, I> entityMappingConfiguration,
																		 Table targetTable,
																		 TableNamingStrategy tableNamingStrategy) {
		Map<EntityMappingConfiguration, Table> result = new HashMap<>();
		
		entityMappingConfiguration.inheritanceIterable().forEach(new Consumer<EntityMappingConfiguration>() {
			
			private Table currentTable = targetTable;
			
			@Override
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				InheritanceConfiguration<?, ?> inheritanceConfiguration = entityMappingConfiguration.getInheritanceConfiguration();
				boolean changeTable = nullable(inheritanceConfiguration)
						.map(InheritanceConfiguration::isJoinTable).getOr(false);
				result.put(entityMappingConfiguration, currentTable);
				if (changeTable) {
					currentTable = nullable(inheritanceConfiguration.getTable())
							.getOr(() -> new Table(tableNamingStrategy.giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
				}
			}
		});
		return result;
	}
}
