package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;

import org.gama.lang.Reflections;
import org.gama.lang.Strings;

/**
 * @author Guillaume Mary
 */
public interface JoinColumnNamingStrategy {
	
	String DEFAULT_JOIN_COLUMN_SUFFIX = "Id";
	
	/**
	 * Expected to generate a name for the column that will join on the identifier of another one.
	 * @param accessor the method (getter or setter) of the entity relation
	 */
	String giveName(Method accessor);
	
	JoinColumnNamingStrategy DEFAULT = accessor ->
			Strings.uncapitalize(Reflections.JAVA_BEAN_ACCESSOR_PREFIX_REMOVER.apply(accessor)) + DEFAULT_JOIN_COLUMN_SUFFIX;
	
}
