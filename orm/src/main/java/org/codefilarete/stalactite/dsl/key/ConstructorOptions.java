package org.codefilarete.stalactite.dsl.key;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.TriFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Interface for {@link org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder#mapKey(SerializableFunction, IdentifierPolicy)} family methods return.
 * Aimed at chaining to configure entity key mapping.
 *
 * @param <C> entity type
 * @param <I> identifier type
 */
public interface ConstructorOptions<C, I, SELF extends ConstructorOptions<C ,I, SELF>> {
	
	/**
	 * Indicates a no-arg factory to be used to instantiate entity.
	 *
	 * @param factory any no-arg method returning an instance of C
	 * @return this
	 */
	SELF usingConstructor(Supplier<C> factory);
	
	/**
	 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given primary key value.
	 *
	 * @param factory 1-arg constructor to be used
	 * @return this
	 */
	SELF usingConstructor(Function<? super I, C> factory);
	
	/**
	 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given column value (expected to be table primary key).
	 *
	 * @param factory 1-arg constructor to be used
	 * @param input column to use for retrieving value to be given as constructor argument
	 * @return this
	 */
	<T extends Table<T>> SELF usingConstructor(Function<? super I, C> factory, Column<T, I> input);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with only column name.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName name of column to use for retrieving value to be given as constructor argument
	 */
	SELF usingConstructor(Function<? super I, C> factory, String columnName);
	
	/**
	 * Variant of {@link #usingConstructor(Function, Column)} with 2 {@link Column}s : first one is expected to be primary key,
	 * while second one is an extra data.
	 * About extra data : if it has an equivalent property, then it should be marked as set by constructor to avoid a superfluous
	 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, its mapping declaration ({@link PropertyOptions#column(Column)})
	 * should use this column as argument to make whole mapping consistent.
	 *
	 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 first column to use for retrieving value to be given as constructor argument
	 * @param input2 second column to use for retrieving value to be given as constructor argument
	 */
	<X, T extends Table<T>> SELF usingConstructor(BiFunction<? super I, X, C> factory,
												  Column<T, I> input1,
												  Column<T, X> input2);
	
	/**
	 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with only column names.
	 *
	 * @param factory constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 first column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName2 second column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 */
	<X> SELF usingConstructor(BiFunction<? super I, X, C> factory, String columnName1, String columnName2);
	
	/**
	 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with 3 {@link Column}s : first one is expected to be primary key,
	 * while second and third one are extra data.
	 * About extra data : if they have equivalent properties, then they should be marked as set by constructor to avoid a superfluous
	 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, their mapping declarations ({@link PropertyOptions#column(Column)})
	 * should use the columns as argument to make whole mapping consistent.
	 *
	 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 first column to use for retrieving value to be given as constructor argument
	 * @param input2 second column to use for retrieving value to be given as constructor argument
	 * @param input3 third column to use for retrieving value to be given as constructor argument
	 */
	<X, Y, T extends Table<T>> SELF usingConstructor(TriFunction<? super I, X, Y, C> factory,
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
	<X, Y> SELF usingConstructor(TriFunction<? super I, X, Y, C> factory,
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
	 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} and {@link PropertyOptions#column(Column)}.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 */
	// signature note : the generics wildcard ? are actually expected to be of same type, but left it as it is because setting a generics type
	// for it makes usage of Function::apply quite difficult (lot of cast) because the generics type can hardly be something else than Object
	SELF usingFactory(Function<Function<Column<?, ?>, ?>, C> factory);
	
}
