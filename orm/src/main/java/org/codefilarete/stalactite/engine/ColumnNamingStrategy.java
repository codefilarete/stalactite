package org.codefilarete.stalactite.engine;

import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;
import org.codefilarete.reflection.AccessorDefinition;

import static org.codefilarete.tool.Reflections.GET_SET_PREFIX_REMOVER;
import static org.codefilarete.tool.Reflections.IS_PREFIX_REMOVER;

/**
 * @author Guillaume Mary
 */
public interface ColumnNamingStrategy {
	
	/**
	 * Expected to generate a name for the given method definition that maps a property to a column
	 * 
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the property to be persisted
	 */
	String giveName(AccessorDefinition accessorDefinition);
	
	/**
	 * Strategy to give property name as the column name, property name is taken from method according to the Java Bean naming convention
	 */
	ColumnNamingStrategy DEFAULT = accessor -> Strings.uncapitalize(Reflections.onJavaBeanPropertyWrapperNameGeneric(accessor.getName(), accessor.getName(),
			GET_SET_PREFIX_REMOVER, GET_SET_PREFIX_REMOVER, IS_PREFIX_REMOVER, s -> s));
	
	/**
	 * Default naming for index column in one-to-many {@link java.util.List} association
	 */
	ColumnNamingStrategy INDEX_DEFAULT = accessor -> "idx";
	
}
