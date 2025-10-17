package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.engine.configurer.builder.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.builder.BeanMappingBuilder.BeanMappingConfiguration.Linkage;
import org.codefilarete.tool.Strings;

/**
 * Strategy for generating index names from a property mapping.
 * Used by persister builder when creating indexes for properties marked with {@link org.codefilarete.stalactite.dsl.property.PropertyOptions#unique()}.
 *
 * @see BeanMappingBuilder
 * @see AccessorDefinition
 */
public interface IndexNamingStrategy {
	
	String giveName(Linkage<?, ?> linkage);
	
	IndexNamingStrategy DEFAULT = new SnakeCaseIndexNamingStrategy();
	
	class SnakeCaseIndexNamingStrategy implements IndexNamingStrategy {
		
		public static final String DEFAULT_SUFFIX = "key";
		
		@Override
		public String giveName(Linkage<?, ?> linkage) {
			if (linkage.getColumnName() != null) {
				return linkage.getColumnName() + DEFAULT_SUFFIX;
			} else {
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(linkage.getAccessor());
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
