package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.function.Serie;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
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
public interface IFluentMappingBuilder<C extends Identified, I extends StatefullIdentifier> extends IFluentEmbeddableMappingConfiguration<C> {
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, ?> function, String columnName);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column);
	
	IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, ClassMappingStrategy<? super C, ?, ?> mappingStrategy);
	
	IFluentMappingBuilder<C, I> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy);
	
	<O extends Identified, J extends StatefullIdentifier>
	IFluentMappingBuilderOneToOneOptions<C, I>
	addOneToOne(SerializableFunction<C, O> getter, Persister<O, J, ? extends Table> persister);
	
	/**
	 * Declares a relationship between an entity of type {@code T} and a {@link Set} of entities of type {@code O}.
	 * This method is dedicated to {@link Set} because generic types are erased so you can't defined a generic method and refine return type
	 * and arguments in order to distinct it from a {@link List} version.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param persister the persister of the {@link Set} entities 
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Set} type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 * @see #addOneToManyList(SerializableFunction, Persister)
	 */
	<O extends Identified, J extends StatefullIdentifier, S extends Collection<O>>
	IFluentMappingBuilderOneToManyOptions<C, I, O>
	addOneToManySet(SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister);
	
	/**
	 * Declares a relationship between an entity of type {@code T} and a {@link List} of entities of type {@code O}.
	 * This method is dedicated to {@link List} because generic types are erased so you can't defined a generic method and refine return type
	 * and arguments in order to distinct it from a {@link Set} version.
	 * 
	 * @param getter the way to get the {@link List} from source entities
	 * @param persister the persister of the {@link List} entities 
	 * @param <O> type of {@link List} element
	 * @param <J> type of identifier of {@code O} (target entities)
	 * @param <S> refined {@link List} type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 * @see #addOneToManySet(SerializableFunction, Persister)
	 */
	<O extends Identified, J extends StatefullIdentifier, S extends List<O>>
	IFluentMappingBuilderOneToManyListOptions<C, I, O>
	addOneToManyList(SerializableFunction<C, S> getter, Persister<O, J, ? extends Table> persister);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I> embed(SerializableBiConsumer<C, O> setter);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I> embed(SerializableFunction<C, O> getter);
	
	IFluentMappingBuilder<C, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	IFluentMappingBuilder<C, I> joinColumnNamingStrategy(JoinColumnNamingStrategy columnNamingStrategy);
	
	<V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter);
	
	<V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> sequence);
	
	/**
	 * Final method, builds the concrete persistence mapping
	 * 
	 * @param dialect the {@link Dialect} to build the mapping for
	 * @return the finalized mapping, can be modified further
	 */
	ClassMappingStrategy<C, I, Table> build(Dialect dialect);
	
	<CC extends Table> EmbeddedBeanMappingStrategy<C, CC> buildEmbeddable(Dialect dialect);
		
	Persister<C, I, ?> build(PersistenceContext persistenceContext);
	
	interface IFluentMappingBuilderColumnOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToOneOptions<T extends Identified, I extends StatefullIdentifier> extends IFluentMappingBuilder<T, I>, OneToOneOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified> extends IFluentMappingBuilder<T, I>, OneToManyOptions<T, I, O> {
	}
	
	/**
	 * A merge of {@link IFluentMappingBuilderOneToManyOptions} and {@link IndexableCollectionOptions} to defined a one-to-many relationship
	 * with a indexed {@link java.util.Collection} such as a {@link List}
	 * 
	 * @param <T> type of source entity
	 * @param <I> type of identifier of source entity
	 * @param <O> type of target entities
	 */
	interface IFluentMappingBuilderOneToManyListOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified>
			extends IFluentMappingBuilderOneToManyOptions<T, I, O>, IndexableCollectionOptions<T, I, O> {
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<T, I, O> mappedBy(SerializableBiConsumer<O, T> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<T, I, O> mappedBy(SerializableFunction<O, T> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<T, I, O> mappedBy(Column<Table, T> reverseLink);
		
		/**
		 * Defines the indexing column of the mapped {@link java.util.List}.
		 * @param orderingColumn indexing column of the mapped {@link java.util.List}
		 * @return the global mapping configurer
		 */
		IFluentMappingBuilderOneToManyListOptions<T, I, O> indexedBy(Column orderingColumn);
	}
	
	interface IFluentMappingBuilderEmbedOptions<T extends Identified, I extends StatefullIdentifier>
			extends IFluentMappingBuilder<T, I>, IFluentEmbeddableMappingConfigurationEmbedOptions<T>, EmbedWithColumnOptions<T> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<T, I> overrideName(SerializableFunction<IN, ?> function, String columnName);
		
		/**
		 * Overrides embedding with an existing target column
		 *
		 * @param function the getter as a method reference
		 * @param targetColumn a column that's the target of the getter
		 * @param <IN> input of the function (type of the embedded element)
		 * @param <OUT> ouput of the function (property type)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN, OUT> IFluentMappingBuilderEmbedOptions<T, I> override(SerializableFunction<IN, OUT> function, Column<Table, OUT> targetColumn);
		
	}
	
}
