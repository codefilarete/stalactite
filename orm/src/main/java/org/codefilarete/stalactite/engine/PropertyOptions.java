package org.codefilarete.stalactite.engine;

import java.util.function.Function;

import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.KeyOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;

/**
 * Options on a basic property
 * 
 * @author Guillaume Mary
 */
public interface PropertyOptions<O> {
	
	/**
	 * Marks the property as mandatory. Note that using this method on an identifier one as no purpose because
	 * identifiers are already mandatory.
	 */
	PropertyOptions<O> mandatory();
	
	/**
	 * Marks this property as set by constructor, meaning it won't be set by any associated setter (method or field access).
	 * Should be used in conjunction with {@link KeyOptions#usingConstructor(Function)}
	 * and other equivalent methods.
	 */
	PropertyOptions<O> setByConstructor();
	
	/**
	 * Marks this property as read-only, making its column not writable (but property is still writable to let queries
	 * set its value)
	 */
	PropertyOptions<O> readonly();
	
	/**
	 * Sets column name to be used. By default, column name is deduced from property name (it is deduced from
	 * property accessor), this method overwrites {@link ColumnNamingStrategy} for this property as well as field name
	 * (see {@link #fieldName(String)}.
	 */
	PropertyOptions<O> columnName(String name);
	
	/**
	 * Sets column to be used. Used to target a specific {@link Column} took on a {@link Table} created upstream.
	 * Allows to overwrite {@link ColumnNamingStrategy} as well as column Java type for this property (and maybe
	 * column property SQL type if you registered it to dialect {@link org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry}.
	 * This also sets {@link Table} to be used by mapping.
	 * 
	 * @param column {@link Column} to be written and read by this property
	 */
	PropertyOptions<O> column(Column<? extends Table, ? extends O> column);
	
	/**
	 * Sets {@link java.lang.reflect.Field} name targeted by this property. Overwrites default mechanism which
	 * deduces it from accessor name.
	 * Uses it if your accessor doesn't follow bean naming convention.
	 * Field name will be used as column name except if {@link #columnName(String)} is used, it also overwrites
	 * {@link ColumnNamingStrategy} for this property.
	 * 
	 * @param name {@link java.lang.reflect.Field} name that stores property value
	 */
	PropertyOptions<O> fieldName(String name);
	
	/**
	 * Applies some converter onto data coming from database before setting it to the property
	 * @param converter the converter that will adapt database value
	 */
	PropertyOptions<O> readConverter(Converter<O, O> converter);
	
	/**
	 * Applies some converter onto property value before writing it to the database
	 * @param converter the converter that will adapt property value
	 */
	PropertyOptions<O> writeConverter(Converter<O, O> converter);
	
	/**
	 * Sets a low-level adapter of read/written value from/to the database. To be used when property type and column
	 * type doesn't match. For example when the property is a date and the column is some character sequence.
	 * 
	 * @param parameterBinder the converter that will adapt database value
	 * @param <V> type of the column in the database
	 */
	<V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder);
	
}
