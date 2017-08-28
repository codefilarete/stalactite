package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.function.Serie;
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
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function, String columnName);
	
	IFluentMappingBuilder<T, I> add(Function<T, ?> function, String columnName);
	
	<O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(Function<T, O> function, Persister<O, J> persister);
	
	<O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> IFluentMappingBuilderOneToManyOptions<T, I, O> addOneToMany(Function<T, C> function, Persister<O, J> persister);
	
	IFluentMappingBuilder<T, I> embed(Function<T, ?> function);
	
	IFluentMappingBuilder<T, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	IFluentMappingBuilder<T, I> joinColumnNamingStrategy(JoinColumnNamingStrategy columnNamingStrategy);
	
	<C> IFluentMappingBuilder<T, I> versionedBy(Function<T, C> property);
	
	<C> IFluentMappingBuilder<T, I> versionedBy(Function<T, C> property, Serie<C> sequence);
	
	ClassMappingStrategy<T, I> build(Dialect dialect);
	
	Persister<T, I> build(PersistenceContext persistenceContext);
	
	interface IFluentMappingBuilderColumnOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToOneOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, OneToOneOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified> extends IFluentMappingBuilder<T, I>, OneToManyOptions<T, I, O> {
	}
	
}
