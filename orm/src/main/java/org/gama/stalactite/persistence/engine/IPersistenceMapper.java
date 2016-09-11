package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface IPersistenceMapper<T> {
	
	<I> IPersistenceMapperColumnOptions<T> add(BiConsumer<T, I> function);
	
	IPersistenceMapperColumnOptions<T> add(Function<T, ?> function);
	
	IPersistenceMapper<T> add(Function<T, ?> function, String columnName);
	
	<I> ClassMappingStrategy<T, I> forDialect(Dialect dialect);
	
	interface IPersistenceMapperColumnOptions<T> extends IPersistenceMapper<T>, ColumnOptions<T> {
	}
	
}
