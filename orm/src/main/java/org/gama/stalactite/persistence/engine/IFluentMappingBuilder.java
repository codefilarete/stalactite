package org.gama.stalactite.persistence.engine;

import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.function.Serie;
import org.gama.reflection.AccessorChain;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class. 
 * Please note that it can't extend {@link IFluentEmbeddableMappingBuilder} because it clashes on the {@link #build(PersistenceContext)} methods that don't
 * have compatible return type.
 * 
 * @author Guillaume Mary
 * @see FluentEntityMappingConfigurationSupport#from(Class, Class)
 * @see #build(PersistenceContext)
 */
public interface IFluentMappingBuilder<C, I> extends IFluentEmbeddableMappingConfiguration<C>, PersisterBuilder<C, I> {
	
	/* Overwritting methods signature to return a type that aggregates options of this class */
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, String columnName);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableBiConsumer<C, O> getter, Column<Table, O> column);
	
	<O> IFluentMappingBuilderColumnOptions<C, I> add(SerializableFunction<C, O> getter, Column<Table, O> column);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> getter, Column<Table, E> column);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<Table, E> column);
	
	IFluentMappingBuilder<C, I> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Declares the inherited mapping. Id policy must be defined in the given strategy.
	 * 
	 * @param mappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	IFluentMappingBuilder<C, I> mapInheritance(EntityMappingConfiguration<? super C, I> mappingConfiguration);
	
	/**
	 * Declares the mapping of a super class.
	 * 
	 * @param superMappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	IFluentMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 * 
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfiguration<O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table target table of the mapped configuration
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfiguration<O, J> mappingConfiguration, T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} throught a {@link Set}.
	 * This method is dedicated to {@link Set} because generic types are erased so you can't defined a generic type extending {@link Set} and refine
	 * return type or arguments in order to distinct it from a {@link List} version.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities 
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Set} type
	 * @return a enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #addOneToManyList(SerializableFunction, EntityMappingConfiguration)
	 */
	<O, J, S extends Set<O>>
	IFluentMappingBuilderOneToManyOptions<C, I, O>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfiguration<O, J> mappingConfiguration);
	
	<O, J, S extends Set<O>, T extends Table>
	IFluentMappingBuilderOneToManyOptions<C, I, O>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfiguration<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends Set<O>, T extends Table>
	IFluentMappingBuilderOneToManyOptions<C, I, O>
	addOneToManySet(SerializableBiConsumer<C, S> setter, EntityMappingConfiguration<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} throught a {@link List}.
	 * This method is dedicated to {@link List} because generic types are erased so you can't defined a generic type extending {@link List} and refine
	 * return type or arguments in order to distinct it from a {@link Set} version.
	 * 
	 * @param getter the way to get the {@link List} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link List} entities 
	 * @param <O> type of {@link List} element
	 * @param <J> type of identifier of {@code O} (target entities)
	 * @param <S> refined {@link List} type
	 * @return a enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #addOneToManySet(SerializableFunction, EntityMappingConfiguration)
	 */
	<O, J, S extends List<O>>
	IFluentMappingBuilderOneToManyListOptions<C, I, O>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfiguration<O, J> mappingConfiguration);
	
	<O, J, S extends List<O>, T extends Table>
	IFluentMappingBuilderOneToManyListOptions<C, I, O>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfiguration<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends List<O>, T extends Table>
	IFluentMappingBuilderOneToManyListOptions<C, I, O>
	addOneToManyList(SerializableBiConsumer<C, S> setter, EntityMappingConfiguration<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter);
	
	@Override
	<O> IFluentMappingBuilderEmbeddableOptions<C, I, O> embed(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	@Override
	<O> IFluentMappingBuilderEmbeddableOptions<C, I, O> embed(SerializableBiConsumer<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	IFluentMappingBuilder<C, I> foreignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	IFluentMappingBuilder<C, I> joinColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	<V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter);
	
	<V> IFluentMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> sequence);
	
	IFluentMappingBuilder<C, I> withJoinTable();
	
	IFluentMappingBuilder<C, I> withJoinTable(Table parentTable);
	
	interface IFluentMappingBuilderColumnOptions<T, I> extends IFluentMappingBuilder<T, I>, ColumnOptions<T, I> {
	}
	
	interface IFluentMappingBuilderOneToOneOptions<C, I, T extends Table> extends IFluentMappingBuilder<C, I>, OneToOneOptions<C, I, T> {
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @param <O> owner type
		 * @return the global mapping configurer
		 */
		@Override
		<O> IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableBiConsumer<O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (getter)
		 * @param <O> owner type
		 * @return the global mapping configurer
		 */
		@Override
		<O> IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableFunction<O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(Column<T, C> reverseLink);
	}
	
	interface IFluentMappingBuilderOneToManyOptions<T, I, O> extends IFluentMappingBuilder<T, I>, OneToManyOptions<T, I, O> {
	}
	
	/**
	 * A merge of {@link IFluentMappingBuilderOneToManyOptions} and {@link IndexableCollectionOptions} to defined a one-to-many relation
	 * with a indexed {@link java.util.Collection} such as a {@link List}
	 * 
	 * @param <C> type of source entity
	 * @param <I> type of identifier of source entity
	 * @param <O> type of target entities
	 */
	interface IFluentMappingBuilderOneToManyListOptions<C, I, O>
			extends IFluentMappingBuilderOneToManyOptions<C, I, O>, IndexableCollectionOptions<C, I, O> {
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O> mappedBy(SerializableBiConsumer<O, C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O> mappedBy(SerializableFunction<O, C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O> mappedBy(Column<Table, C> reverseLink);
		
		/**
		 * Defines the indexing column of the mapped {@link java.util.List}.
		 * @param orderingColumn indexing column of the mapped {@link java.util.List}
		 * @return the global mapping configurer
		 */
		<T extends Table> IFluentMappingBuilderOneToManyListOptions<C, I, O> indexedBy(Column<T, Integer> orderingColumn);
	}
	
	interface IFluentMappingBuilderEmbedOptions<C, I, O>
			extends IFluentMappingBuilder<C, I>, IFluentEmbeddableMappingConfigurationEmbedOptions<C, O>, EmbedWithColumnOptions<O> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		/**
		 * Overrides embedding with an existing target column
		 *
		 * @param function the getter as a method reference
		 * @param targetColumn a column that's the target of the getter
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> override(SerializableFunction<O, IN> function, Column<Table, IN> targetColumn);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> exclude(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping" 
	 * @param <C>
	 * @param <I>
	 * @param <O>
	 */
	interface IFluentMappingBuilderEmbeddableOptions<C, I, O>
		extends IFluentMappingBuilder<C, I>,
			IFluentEmbeddableMappingConfigurationEmbeddableOptions<C, O> {
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableOptions<C, I, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableOptions<C, I, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableOptions<C, I, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
	}
	
	interface IFluentMappingBuilderEnumOptions<C, I>
			extends IFluentEmbeddableMappingConfigurationEnumOptions<C>,
			IFluentMappingBuilder<C, I> {
		
		@Override
		IFluentMappingBuilderEnumOptions<C, I> byName();
		
		@Override
		IFluentMappingBuilderEnumOptions<C, I> byOrdinal();
		
	}
}
