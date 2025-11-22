package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.configurer.builder.embeddable.EmbeddableMappingBuilder;
import org.codefilarete.tool.Strings;

import javax.annotation.Nullable;

/**
 * Strategy for generating index names from a property mapping.
 * Used by persister builder when creating indexes for properties marked with {@link org.codefilarete.stalactite.dsl.property.PropertyOptions#unique()}.
 *
 * @see EmbeddableMappingBuilder
 * @see AccessorDefinition
 */
public interface IndexNamingStrategy {
	
	default String giveName(Linkage<?, ?> linkage) {
		return giveName(linkage.getAccessor(), linkage.getColumnName());
	}
	
	String giveName(ValueAccessPoint<?> propertyAccessor, @Nullable String columnName);
	
	IndexNamingStrategy DEFAULT = new SnakeCaseIndexNamingStrategy();
	
	class SnakeCaseIndexNamingStrategy implements IndexNamingStrategy {
		
		public static final String DEFAULT_SUFFIX = "key";
		
		@Override
		public String giveName(ValueAccessPoint<?> propertyAccessor, @Nullable String columnName) {
			if (columnName != null) {
				return columnName + "_" + DEFAULT_SUFFIX;
			} else {
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(propertyAccessor);
				// we create a unique name because most of the time (always), database index names have a schema scope,
				// not a table one, thus their uniqueness must be on that scope too.
				return Strings.snakeCase(
						accessorDefinition.getDeclaringClass().getSimpleName()
								+ "_" + accessorDefinition.getName()
								+ "_" + DEFAULT_SUFFIX);
			}
		}
	}
}
