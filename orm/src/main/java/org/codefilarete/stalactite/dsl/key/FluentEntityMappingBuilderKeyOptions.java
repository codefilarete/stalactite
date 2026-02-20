package org.codefilarete.stalactite.dsl.key;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.TriFunction;

public interface FluentEntityMappingBuilderKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, KeyOptions<C, I> {
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Supplier<C> factory);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<I, C> factory);
	
	@Override
	<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<I, C> factory, Column<T, I> input);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<I, C> factory, String columnName);
	
	@Override
	<X, T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																						Column<T, I> input1,
																						Column<T, X> input2);
	
	@Override
	<X> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<I, X, C> factory,
																	String columnName1,
																	String columnName2);
	
	@Override
	<X, Y, T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<I, X, Y, C> factory,
																						   Column<T, I> input1,
																						   Column<T, X> input2,
																						   Column<T, Y> input3);
	
	@Override
	<X, Y> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<I, X, Y, C> factory,
																	   String columnName1,
																	   String columnName2,
																	   String columnName3);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingFactory(Function<ColumnedRow, C> factory);

	/**
	 * Sets column name to be used. By default, column name is deduced from property name (it is deduced from
	 * property accessor), this method overwrites {@link ColumnNamingStrategy} for this property as well as field name
	 * (see {@link #fieldName(String)}.
	 */
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> columnName(String name);

	/**
	 * Sets column size to be used.
	 */
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> columnSize(Size size);

	/**
	 * Sets column to be used. Used to target a specific {@link Column} took on a {@link Table} created upstream.
	 * Allows to overwrite {@link ColumnNamingStrategy} as well as column Java type for this property (and maybe
	 * column property SQL type if you registered it to dialect {@link org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry}.
	 * This also sets {@link Table} to be used by mapping.
	 *
	 * @param column {@link Column} to be written and read by this property
	 */
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> column(Column<? extends Table, ? extends I> column);

	/**
	 * Sets {@link java.lang.reflect.Field} name targeted by this property. Overwrites default mechanism which
	 * deduces it from accessor name.
	 * Uses it if your accessor doesn't follow bean naming convention.
	 * Field name will be used as column name except if {@link #columnName(String)} is used, it also overwrites
	 * {@link ColumnNamingStrategy} for this property.
	 *
	 * @param name {@link java.lang.reflect.Field} name that stores property value
	 */
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> fieldName(String name);
}
