package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.Strings;

import static org.codefilarete.tool.Reflections.GET_SET_PREFIX_REMOVER;
import static org.codefilarete.tool.Reflections.IS_PREFIX_REMOVER;

/**
 * Table naming strategy contract for element collection table
 * 
 * @author Guillaume Mary
 */
public interface ElementCollectionTableNamingStrategy {
	
	/**
	 * Gives association table name
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the collection to be persisted
	 * @return table name for element collection table
	 */
	String giveName(AccessorDefinition accessorDefinition);
	
	ElementCollectionTableNamingStrategy DEFAULT = new DefaultElementCollectionTableNamingStrategy();
	
	/**
	 * Default implementation of the {@link ElementCollectionTableNamingStrategy} interface.
	 * Will use relation property name for it, prefixed with source table name.
	 * For instance: for a Country entity with the getCities() getter to retrieve country cities,
	 * the association table will be named "Country_cities".
	 * If property cannot be deduced from getter (it doesn't start with "get") then target table name will be used, suffixed by "s".
	 * For instance: for a Country entity with the giveCities() getter to retrieve country cities,
	 * the association table will be named "Country_Citys".
	 */
	class DefaultElementCollectionTableNamingStrategy implements ElementCollectionTableNamingStrategy {
		
		@Override
		public String giveName(AccessorDefinition accessor) {
			String suffix = Reflections.onJavaBeanPropertyWrapperNameGeneric(accessor.getName(), accessor.getName(),
					GET_SET_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					GET_SET_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					IS_PREFIX_REMOVER.andThen(Strings::uncapitalize),
					methodName -> methodName);	// on non-compliant Java Bean Naming Convention, method name is returned as table name
			return accessor.getDeclaringClass().getSimpleName() + "_" + suffix;
		}
	}
	
}
