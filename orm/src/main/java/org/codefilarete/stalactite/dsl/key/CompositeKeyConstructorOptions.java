package org.codefilarete.stalactite.dsl.key;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.TriFunction;

/**
 * A placeholder class of eventual future options on composite keys
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public interface CompositeKeyConstructorOptions<C, I> {
	
	/**
	 * Indicates a no-arg factory to be used to instantiate entity.
	 *
	 * @param factory any no-arg method returning an instance of C
	 * @return this   
	 */
	CompositeKeyConstructorOptions<C, I> usingConstructor(Supplier<C> factory);
	
	/**
	 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given column value (expected to be table primary key).
	 *
	 * @param factory 1-arg constructor to be used
	 * @param input column to use for retrieving value to be given as constructor argument
	 * @return this   
	 */
	<X, T extends Table> CompositeKeyConstructorOptions<C, I> usingConstructor(Function<X, C> factory, Column<T, X> input);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with only column name.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName column name to read from {@link java.sql.ResultSet} and pass its value as factory's argument
	 * @return this   
	 */
	<X> CompositeKeyConstructorOptions<C, I> usingConstructor(Function<X, C> factory, String columnName);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with 2 {@link Column}s : columns are expected to compose the composite primary key
	 *
	 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param column1 first column to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param column2 second column to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @return this   
	 */
	<X, Y, T extends Table> CompositeKeyConstructorOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																					 Column<T, X> column1,
																					 Column<T, Y> column2);
	
	/**
	 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with only column names.
	 *
	 * @param factory constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param columnName2 second column name to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @return this   
	 */
	<X, Y> CompositeKeyConstructorOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																 String columnName1,
																 String columnName2);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with 3 {@link Column}s : columns are expected to compose the composite primary key
	 *
	 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param column1 first column to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param column2 second column to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @param column3 third column to read from {@link java.sql.ResultSet} and pass its value as factory's third argument
	 * @return this   
	 */
	<X, Y, Z, T extends Table> CompositeKeyConstructorOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																					 Column<T, X> column1,
																					 Column<T, Y> column2,
																					 Column<T, Z> column3);
	
	/**
	 * Variant of {@link #usingConstructor(TriFunction, Column, Column, Column)} with only column names.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param columnName2 second column name to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @param columnName3 third column name to read from {@link java.sql.ResultSet} and pass its value as factory's third argument
	 * @return this   
	 */
	<X, Y, Z> CompositeKeyConstructorOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																	String columnName1,
																	String columnName2,
																	String columnName3);
	
	/**
	 * Very open variant of {@link #usingConstructor(Supplier)} that gives a {@link Function} to be used to create instances.
	 * <p>
	 * Given factory gets a <code>Function&lt;ColumnedRow, Object&gt;</code> as unique argument (be aware that as a consequence your
	 * code will depend on {@link ColumnedRow}) which represent a kind of {@link java.sql.ResultSet} readable with {@link org.codefilarete.stalactite.query.model.Selectable}s),
	 * to let one fulfills any property of its instance the way he wants.
	 * <br/>
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @return this   
	 */
	CompositeKeyConstructorOptions<C, I> usingFactory(Function<ColumnedRow, C> factory);
}
