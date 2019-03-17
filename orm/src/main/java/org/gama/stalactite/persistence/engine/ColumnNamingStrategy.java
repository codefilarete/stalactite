package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.lang.Strings;

/**
 * @author Guillaume Mary
 */
public interface ColumnNamingStrategy {
	
	/**
	 * Expected to generate a name for the column that will join on the identifier of another one.
	 * @param accessor the method (getter or setter) of the entity relation
	 */
	String giveName(Method accessor);
	
	String DEFAULT_JOIN_COLUMN_SUFFIX = "Id";
	
	/**
	 * Strategy to give property name as the column name, property name is taken from method according to the Java Bean naming convention
	 */
	ColumnNamingStrategy DEFAULT = accessor -> Strings.uncapitalize(Reflections.JAVA_BEAN_ACCESSOR_PREFIX_REMOVER.apply(accessor));
	
	/**
	 * Adds {@value #DEFAULT_JOIN_COLUMN_SUFFIX} to the {@link #DEFAULT} naming strategy 
	 */
	ColumnNamingStrategy JOIN_DEFAULT = accessor -> DEFAULT.giveName(accessor) + DEFAULT_JOIN_COLUMN_SUFFIX;
	
}
