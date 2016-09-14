package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface IFluentMappingBuilder<T, I> {
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function);
	
	IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function);
	
	IFluentMappingBuilder<T, I> add(Function<T, ?> function, String columnName);
	
	ClassMappingStrategy<T, I> build(Dialect dialect);
	
	interface IFluentMappingBuilderColumnOptions<T, I> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
}
