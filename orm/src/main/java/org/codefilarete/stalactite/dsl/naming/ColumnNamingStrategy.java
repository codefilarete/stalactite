package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.tool.Strings;

/**
 * @author Guillaume Mary
 */
public interface ColumnNamingStrategy {
	
	String DEFAULT_INDEX_COLUMN_NAME = "idx";
	
	/**
	 * Expected to generate a name for the given method definition that maps a property to a column
	 * 
	 * @param propertyAccessor a representation of the method (getter or setter) that gives the property to be persisted
	 */
	String giveName(AccessorDefinition propertyAccessor);
	
	/**
	 * Strategy to give property name as the column name, property name is taken from method according to the Java Bean naming convention
	 */
	ColumnNamingStrategy DEFAULT = propertyAccessor -> {
		// The property might be an AccessorChain which is a more a path than a direct property accessor, that is transformed
		// with dots by AccessorDefinition and is not compatible with databases, hence we change them to underscore (arbitrary character)
		return Strings.uncapitalize(propertyAccessor.getName().replace('.', '_'));
	};
	
	/**
	 * Default naming for index column in one-to-many {@link java.util.List} association
	 */
	ColumnNamingStrategy INDEX_DEFAULT = accessor -> DEFAULT_INDEX_COLUMN_NAME;
	
}
