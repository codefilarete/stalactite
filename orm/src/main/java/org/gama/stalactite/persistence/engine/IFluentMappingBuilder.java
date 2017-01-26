package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface IFluentMappingBuilder<T extends Identified, I extends StatefullIdentifier> {
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function);
	
	IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function);
	
	IFluentMappingBuilder<T, I> add(Function<T, ?> function, String columnName);
	
	<O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilder<T, I> cascade(Function<T, O> function, Persister<O, J> persister);
	
	IFluentMappingBuilder<T, I> embed(Function<T, ?> function);
	
	ClassMappingStrategy<T, I> build(Dialect dialect);
	
	Persister<T, I> build(PersistenceContext persistenceContext);
	
	interface IFluentMappingBuilderColumnOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
}
