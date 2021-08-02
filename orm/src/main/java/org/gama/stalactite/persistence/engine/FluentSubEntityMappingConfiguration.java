package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare a sub-entity (from polymorphism point of view) mapping of a class.
 * As a difference with {@link FluentEntityMappingBuilder}, there's no need to declare a build(..) method for it because it only acts as a
 * configuration storage.
 *
 * @author Guillaume Mary
 * @see MappingEase#entityBuilder(Class, Class)
 */
public interface FluentSubEntityMappingConfiguration<C, I> extends SubEntityMappingConfiguration<C> {
	
	/* Overwritting methods signature to return a type that aggregates options of this class */
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<? extends Table, O> column);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType,
																															EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType,
																															EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	FluentSubEntityMappingConfiguration<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 *
	 * @param setter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
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
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param setter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table target table of the mapped configuration
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
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
	 * @see #addOneToManyList(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Set<O>>
	IFluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	<O, J, S extends Set<O>, T extends Table>
	IFluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends Set<O>, T extends Table>
	IFluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
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
	 * @see #addOneToManySet(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends List<O>>
	FluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	<O, J, S extends List<O>, T extends Table>
	FluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends List<O>, T extends Table>
	FluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																							  EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																							  EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	FluentSubEntityMappingConfiguration<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy);
	
	interface FluentSubEntityMappingBuilderPropertyOptions<C, I> extends FluentSubEntityMappingConfiguration<C, I>, ColumnOptions<C, I> {
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I> identifier(IdentifierPolicy identifierPolicy);
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I> mandatory();
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I> setByConstructor();
	}
	
	interface FluentSubEntityMappingConfigurationEnumOptions<C, I> extends FluentSubEntityMappingConfiguration<C, I>, EnumOptions {
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I> byName();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I> byOrdinal();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I> mandatory();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I> setByConstructor();
	}
	
	interface FluentMappingBuilderOneToOneOptions<C, I, T extends Table> extends FluentSubEntityMappingConfiguration<C, I>,
			OneToOneOptions<C, I, T> {
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @param <O> owner type
		 * @return the global mapping configurer
		 */
		@Override
		<O> FluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableBiConsumer<O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (getter)
		 * @param <O> owner type
		 * @return the global mapping configurer
		 */
		@Override
		<O> FluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(SerializableFunction<O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T> mappedBy(Column<T, I> reverseLink);
		
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T> cascading(RelationMode relationMode);
	}
	
	interface FluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends FluentSubEntityMappingConfiguration<C, I>, OneToManyOptions<C, I, O, S> {
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink);
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode);
		
	}
	
	/**
	 * A merge of {@link FluentMappingBuilderOneToManyOptions} and {@link IndexableCollectionOptions} to defined a one-to-many relation
	 * with a indexed {@link java.util.Collection} such as a {@link List}
	 *
	 * @param <C> type of source entity
	 * @param <I> type of identifier of source entity
	 * @param <O> type of target entities
	 */
	interface FluentMappingBuilderOneToManyListOptions<C, I, O, S extends List<O>>
			extends FluentMappingBuilderOneToManyOptions<C, I, O, S>, IndexableCollectionOptions<C, I, O> {
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink);
		
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
		
		/**
		 * Defines the indexing column of the mapped {@link java.util.List}.
		 * @param orderingColumn indexing column of the mapped {@link java.util.List}
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
		
		@Override
		FluentMappingBuilderOneToManyListOptions<C, I, O, S> cascading(RelationMode relationMode);
	}
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping" 
	 * @param <C>
	 * @param <I>
	 * @param <O>
	 */
	interface FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>
			extends FluentSubEntityMappingConfiguration<C, I>, ImportedEmbedWithColumnOptions<O> {
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		/**
		 * Overrides embedding with an existing target column
		 *
		 * @param getter the getter as a method reference
		 * @param targetColumn a column that's the target of the getter
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> override(SerializableFunction<O, IN> getter, Column<? extends Table, IN> targetColumn);
		
		/**
		 * Overrides embedding with an existing target column
		 *
		 * @param setter the setter as a method reference
		 * @param targetColumn a column that's the target of the getter
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> override(SerializableBiConsumer<O, IN> setter, Column<? extends Table, IN> targetColumn);
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> exclude(SerializableFunction<O, IN> getter);
	}
	
	interface FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S extends Collection<O>>
			extends FluentSubEntityMappingConfiguration<C, I>, ElementCollectionOptions<C, O, S> {
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> override(String columnName);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withTable(Table table);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withTable(String tableName);
	}
	
	interface FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S extends Collection<O>>
			extends FluentSubEntityMappingConfiguration<C, I>, ElementCollectionOptions<C, O, S> {
		
		<IN> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		<IN> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(Table table);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(String tableName);
	}
	
}
