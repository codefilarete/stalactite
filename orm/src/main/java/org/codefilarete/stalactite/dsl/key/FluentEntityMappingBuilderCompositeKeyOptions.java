package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.TriFunction;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A mashup of {@link FluentEntityMappingBuilder} and {@link CompositeKeyOptions} for the fluent API.
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public interface FluentEntityMappingBuilderCompositeKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, CompositeKeyOptions<C, I> {
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory any no-arg method returning an instance of C
	 * @return the global mapping configurer
	 */
	@Override
	FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(Supplier<C> factory);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory 1-arg constructor to be used
	 * @param input column to use for retrieving value to be given as constructor argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X, T extends Table> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(Function<X, C> factory, Column<T, X> input);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName column name to read from {@link java.sql.ResultSet} and pass its value as factory's argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(Function<X, C> factory, String columnName);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param column1 first column to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param column2 second column to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X, Y, T extends Table> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																									Column<T, X> column1,
																									Column<T, Y> column2);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param columnName2 second column name to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X, Y> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(BiFunction<X, Y, C> factory,
																				String columnName1,
																				String columnName2);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param column1 first column to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param column2 second column to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @param column3 third column to read from {@link java.sql.ResultSet} and pass its value as factory's third argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X, Y, Z, T extends Table> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																									Column<T, X> column1,
																									Column<T, Y> column2,
																									Column<T, Z> column3);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read from {@link java.sql.ResultSet} and pass its value as factory's first argument
	 * @param columnName2 second column name to read from {@link java.sql.ResultSet} and pass its value as factory's second argument
	 * @param columnName3 third column name to read from {@link java.sql.ResultSet} and pass its value as factory's third argument
	 * @return the global mapping configurer   
	 */
	@Override
	<X, Y, Z> FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingConstructor(TriFunction<X, Y, Z, C> factory,
																				   String columnName1,
																				   String columnName2,
																				   String columnName3);
	
	/**
	 * {@inheritDoc}
	 * Declaration overridden to adapt the return type to this class.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @return the global mapping configurer   
	 */
	@Override
	FluentEntityMappingBuilderCompositeKeyOptions<C, I> usingFactory(Function<ColumnedRow, C> factory);
	
}
