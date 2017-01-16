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
	
	<O> IFluentMappingBuilder<T, I> cascade(Function<T, O> function, IFluentMappingBuilder<O, ?> targetFluentMappingBuilder);
	
	ClassMappingStrategy<T, I> build(Dialect dialect);
	
	Persister<T, I> build(PersistenceContext persistenceContext);
	
	interface IFluentMappingBuilderColumnOptions<T, I> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
}
