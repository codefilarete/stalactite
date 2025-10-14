package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Resolve main table (explicit Table or entity config table or naming strategy)
 *
 * @author Guillaume Mary
 */
public class TableLookupStep<C, I> {
	
	Table lookupForTable(EntityMappingConfiguration<C, I> entityMappingConfiguration,
						 TableNamingStrategy tableNamingStrategy) {
		return nullable(entityMappingConfiguration.getTable())
				.getOr(() -> new Table(tableNamingStrategy.giveName(entityMappingConfiguration.getEntityType())));
	}
}
