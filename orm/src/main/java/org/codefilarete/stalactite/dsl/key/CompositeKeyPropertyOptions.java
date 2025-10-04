package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;

/**
 * Options on a composite key property
 * 
 * @author Guillaume Mary
 */
public interface CompositeKeyPropertyOptions {
	
	/**
	 * Sets column name to be used. By default column name is deduced from property name (it is deduced from
	 * property accessor), this method overwrites {@link ColumnNamingStrategy} for this property as well as field name
	 * (see {@link #fieldName(String)}.
	 */
	CompositeKeyPropertyOptions columnName(String name);
	
	/**
	 * Sets {@link java.lang.reflect.Field} name targeted by this property. Overwrites default mechanism which
	 * deduces it from accessor name.
	 * Uses it if your accessor doesn't follow bean naming convention.
	 * Field name will be used as column name except if {@link #columnName(String)} is used, it also overwrites
	 * {@link ColumnNamingStrategy} for this property.
	 * 
	 * @param name {@link java.lang.reflect.Field} name that stores property value
	 */
	CompositeKeyPropertyOptions fieldName(String name);
	
}
