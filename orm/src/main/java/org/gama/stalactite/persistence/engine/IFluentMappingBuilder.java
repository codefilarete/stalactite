package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface IFluentMappingBuilder<T> {
	
	<I> IFluentMappingBuilderColumnOptions<T> add(BiConsumer<T, I> function);
	
	IFluentMappingBuilderColumnOptions<T> add(Function<T, ?> function);
	
	IFluentMappingBuilder<T> add(Function<T, ?> function, String columnName);
	
	<I> ClassMappingStrategy<T, I> build(Dialect dialect);
	
	interface IFluentMappingBuilderColumnOptions<T> extends IFluentMappingBuilder<T>, ColumnOptions<T> {
	}
	
}
