package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Strings;

/**
 * Strategy for generating index names from a property mapping.
 * Used by persister builder when creating indexes for properties marked with {@link org.codefilarete.stalactite.dsl.property.PropertyOptions#unique()}.
 *
 * @see org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder
 * @see AccessorDefinition
 */
public interface UniqueConstraintNamingStrategy {
	
	/**
	 * Gives an index name based on a property and its owning {@link Table}
	 * @param propertyAccessor property to build index name from
	 * @param column column on which the index is applied
	 * @return index name
	 */
	String giveName(ValueAccessPoint<?> propertyAccessor, Column<?, ?> column);

	UniqueConstraintNamingStrategy DEFAULT = new SnakeCaseUniqueConstraintNamingStrategy();
	
	class SnakeCaseUniqueConstraintNamingStrategy implements UniqueConstraintNamingStrategy {
		
		public static final String DEFAULT_SUFFIX = "key";
		
		@Override
		public String giveName(ValueAccessPoint<?> propertyAccessor, Column<?, ?> column) {
			// we create a unique name because most of the time (always), database index names have a schema scope,
			// not a table one, thus their uniqueness must be on that scope too.
			return Strings.snakeCase(
					column.getTable().getName()
							+ "_" + column.getName()
							+ "_" + DEFAULT_SUFFIX);
		}
	}
}
