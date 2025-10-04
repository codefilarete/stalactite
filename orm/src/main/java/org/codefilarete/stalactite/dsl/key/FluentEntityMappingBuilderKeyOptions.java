package org.codefilarete.stalactite.dsl.key;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.TriFunction;

public interface FluentEntityMappingBuilderKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, KeyOptions<C, I> {
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Supplier<C> factory);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory);
	
	@Override
	<T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName);
	
	@Override
	<X, T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																						Column<T, I> input1,
																						Column<T, X> input2);
	
	@Override
	<X> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																	String columnName1,
																	String columnName2);
	
	@Override
	<X, Y, T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																						   Column<T, I> input1,
																						   Column<T, X> input2,
																						   Column<T, Y> input3);
	
	@Override
	<X, Y> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																	   String columnName1,
																	   String columnName2,
																	   String columnName3);
	
	@Override
	FluentEntityMappingBuilderKeyOptions<C, I> usingFactory(Function<Function<Column<?, ?>, ?>, C> factory);
}
