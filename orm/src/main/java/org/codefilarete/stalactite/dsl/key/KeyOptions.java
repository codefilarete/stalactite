package org.codefilarete.stalactite.dsl.key;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.TriFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Interface for {@link org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder#mapKey(SerializableFunction, IdentifierPolicy)} family methods return.
 * Aimed at chaining to configure entity key mapping.
 *
 * @param <C> entity type
 * @param <I> identifier type
 */
public interface KeyOptions<C, I> extends ConstructorOptions<C, I>, PropertyOptions<I> {
	
	/**
	 * Sets column name to be used. By default, column name is deduced from property name (it is deduced from
	 * property accessor), this method overwrites {@link ColumnNamingStrategy} for this property as well as field name
	 * (see {@link #fieldName(String)}.
	 */
	@Override
	KeyOptions<C, I> columnName(String name);
	
	/**
	 * Sets column size to be used.
	 */
	@Override
	KeyOptions<C, I> columnSize(Size size);
	
	/**
	 * Sets column to be used. Used to target a specific {@link Column} took on a {@link Table} created upstream.
	 * Allows to overwrite {@link ColumnNamingStrategy} as well as column Java type for this property (and maybe
	 * column property SQL type if you registered it to dialect {@link org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry}.
	 * This also sets {@link Table} to be used by mapping.
	 *
	 * @param column {@link Column} to be written and read by this property
	 */
	@Override
	KeyOptions<C, I> column(Column<? extends Table, ? extends I> column);
	
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
	KeyOptions<C, I> fieldName(String name);
	
	/**
	 * Indicates a no-arg factory to be used to instantiate entity.
	 *
	 * @param factory any no-arg method returning an instance of C
	 * @return this
	 */
	@Override
	KeyOptions<C, I> usingConstructor(Supplier<C> factory);
	
	/**
	 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given primary key value.
	 *
	 * @param factory 1-arg constructor to be used
	 * @return this
	 */
	@Override
	KeyOptions<C, I> usingConstructor(Function<I, C> factory);
	
	/**
	 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given column value (expected to be table primary key).
	 *
	 * @param factory 1-arg constructor to be used
	 * @param input column to use for retrieving value to be given as constructor argument
	 * @return this
	 */
	@Override
	<T extends Table> KeyOptions<C, I> usingConstructor(Function<I, C> factory, Column<T, I> input);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with only column name.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName name of column to use for retrieving value to be given as constructor argument
	 */
	@Override
	KeyOptions<C, I> usingConstructor(Function<I, C> factory, String columnName);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with 2 {@link Column}s : first one is expected to be primary key,
	 * while second one is an extra data.
	 *
	 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 first column to use for retrieving value to be given as constructor argument
	 * @param input2 second column to use for retrieving value to be given as constructor argument
	 */
	@Override
	<X, T extends Table> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
															  Column<T, I> input1,
															  Column<T, X> input2);
	
	/**
	 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with only column names.
	 *
	 * @param factory constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName2 second column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 */
	@Override
	<X> KeyOptions<C, I> usingConstructor(BiFunction<I, X, C> factory,
										  String columnName1,
										  String columnName2);
	
	/**
	 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with 3 {@link Column}s : first one is expected to be primary key,
	 * while second and third one are extra data.
	 *
	 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 first column to use for retrieving value to be given as constructor argument
	 * @param input2 second column to use for retrieving value to be given as constructor argument
	 * @param input3 third column to use for retrieving value to be given as constructor argument
	 */
	@Override
	<X, Y, T extends Table> KeyOptions<C, I> usingConstructor(TriFunction<I, X, Y, C> factory,
																 Column<T, I> input1,
																 Column<T, X> input2,
																 Column<T, Y> input3);
	
	/**
	 * Variant of {@link #usingConstructor(TriFunction, Column, Column, Column)} with only column names.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName2 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName3 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 */
	@Override
	<X, Y> KeyOptions<C, I> usingConstructor(TriFunction<I, X, Y, C> factory,
											 String columnName1,
											 String columnName2,
											 String columnName3);
	
	/**
	 * Very open variant of {@link #usingConstructor(Function)} that gives a {@link Function} to be used to create instances.
	 * <p>
	 * Given factory gets a <pre>Function<? extends Column, ? extends Object></pre> as unique argument (be aware that as a consequence its
	 * code will depend on {@link Column}) which represent a kind of {@link java.sql.ResultSet} so one can fulfill any property of its instance
	 * the way he wants.
	 * <br/>
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 */
	@Override
	KeyOptions<C, I> usingFactory(Function<ColumnedRow, C> factory);
	
}
