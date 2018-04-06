package org.gama.stalactite.persistence.engine;

import java.util.Collection;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.function.Serie;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class 
 * 
 * @author Guillaume Mary
 * @see FluentMappingBuilder#from(Class, Class)
 * @see FluentMappingBuilder#from(Class, Class, Table)
 * @see #build(Dialect)
 */
public interface IFluentMappingBuilder<T extends Identified, I extends StatefullIdentifier> {
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableBiConsumer<T, O> function);
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, O> function);
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableBiConsumer<T, O> function, String columnName);
	
	IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, ?> function, String columnName);
	
	<O> IFluentMappingBuilderColumnOptions<T, I> add(SerializableFunction<T, O> function, Column<O> column);
	
	<O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(SerializableFunction<T, O> function, Persister<O, J> persister);
	
	<O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> IFluentMappingBuilderOneToManyOptions<T, I, O> addOneToMany(SerializableFunction<T, C> function, Persister<O, J> persister);
	
	<O> IFluentMappingBuilderEmbedOptions<T, I> embed(SerializableBiConsumer<T, O> function);
	
	<O> IFluentMappingBuilderEmbedOptions<T, I> embed(SerializableFunction<T, O> function);
	
	IFluentMappingBuilder<T, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	IFluentMappingBuilder<T, I> joinColumnNamingStrategy(JoinColumnNamingStrategy columnNamingStrategy);
	
	<C> IFluentMappingBuilder<T, I> versionedBy(SerializableFunction<T, C> property);
	
	<C> IFluentMappingBuilder<T, I> versionedBy(SerializableFunction<T, C> property, Serie<C> sequence);
	
	/**
	 * Final method, builds the concrete persistence mapping
	 * 
	 * @param dialect the {@link Dialect} to build the mapping for
	 * @return the finalized mapping, can be modified further
	 */
	ClassMappingStrategy<T, I> build(Dialect dialect);
	
	Persister<T, I> build(PersistenceContext persistenceContext);
	
	interface IFluentMappingBuilderColumnOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToOneOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, OneToOneOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified> extends IFluentMappingBuilder<T, I>, OneToManyOptions<T, I, O> {
	}
	
	interface IFluentMappingBuilderEmbedOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, EmbedOptions<T, I> {
	}
	
}
