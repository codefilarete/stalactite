package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * An interface describing a fluent way to declare a sub-entity (from polymorphism point of view) mapping of a class.
 * As a difference with {@link FluentEntityMappingBuilder}, there's no need to declare a build(..) method for it because it only acts as a
 * configuration storage.
 *
 * @author Guillaume Mary
 * @see MappingEase#entityBuilder(Class, Class)
 */
public interface FluentSubEntityMappingBuilder<C, I> extends SubEntityMappingConfigurationProvider<C> {
	
	/* Overwriting methods signature to return a type that aggregates options of this class */
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I, O> map(SerializableBiConsumer<C, O> setter);
	
	<O> FluentSubEntityMappingBuilderPropertyOptions<C, I, O> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> mapEnum(SerializableFunction<C, E> getter);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter, Class<O> componentType,
																															EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	<O, S extends Collection<O>> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType,
																															EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	FluentSubEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 *
	 * @param setter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
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
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
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
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, T, O> mapOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * This method is dedicated to {@link Set} because generic types are erased so you can't defined a generic type extending {@link Set} and refine
	 * return type or arguments in order to distinct it from a {@link List} version.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities 
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return a enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	<O, J, S extends Collection<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends Collection<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	<O, J, S extends Collection<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter,
																							  EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter,
																							  EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	FluentSubEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy);
	
	interface FluentSubEntityMappingBuilderPropertyOptions<C, I, O> extends FluentSubEntityMappingBuilder<C, I>, ColumnOptions<O> {
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> mandatory();
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> setByConstructor();
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> readonly();
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> columnName(String name);
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> column(Column<? extends Table, ? extends O> column);
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> fieldName(String name);
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> readConverter(Converter<O, O> converter);
		
		@Override
		FluentSubEntityMappingBuilderPropertyOptions<C, I, O> writeConverter(Converter<O, O> converter);
		
		@Override
		<V> FluentSubEntityMappingBuilderPropertyOptions<C, I,O> sqlBinder(ParameterBinder<V> parameterBinder);
	}
	
	interface FluentSubEntityMappingConfigurationEnumOptions<C, I, E extends Enum<E>> extends FluentSubEntityMappingBuilder<C, I>, EnumOptions<E> {
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> byName();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> byOrdinal();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> mandatory();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> setByConstructor();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> readonly();
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> columnName(String name);
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> column(Column<? extends Table, ? extends E> column);
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> fieldName(String name);
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> readConverter(Converter<E, E> converter);
		
		@Override
		FluentSubEntityMappingConfigurationEnumOptions<C, I, E> writeConverter(Converter<E, E> converter);
		
		@Override
		<V> FluentSubEntityMappingConfigurationEnumOptions<C, I, E> sqlBinder(ParameterBinder<V> parameterBinder);
	}
	
	interface FluentMappingBuilderOneToOneOptions<C, I, T extends Table, O> extends FluentSubEntityMappingBuilder<C, I>,
			OneToOneOptions<C, I, T, O> {
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (getter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T, O> mappedBy(Column<T, I> reverseLink);
		
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T, O> cascading(RelationMode relationMode);
		
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T, O> fetchSeparately();
	}
	
	interface FluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends FluentSubEntityMappingBuilder<C, I>, OneToManyOptions<C, I, O, S> {
		
		/**
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * Declaration overridden to adapt return type to this class.
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
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(String columnName);
		
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexed();
		
	}
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping" 
	 * @param <C>
	 * @param <I>
	 * @param <O>
	 */
	interface FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>
			extends FluentSubEntityMappingBuilder<C, I>, ImportedEmbedWithColumnOptions<O> {
		
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
			extends FluentSubEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> override(String columnName);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withReverseJoinColumn(String name);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withTable(Table table);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionOptions<C, I, O, S> withTable(String tableName);
	}
	
	interface FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S extends Collection<O>>
			extends FluentSubEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		<IN> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		<IN> FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withReverseJoinColumn(String name);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(Table table);
		
		@Override
		FluentSubEntityMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(String tableName);
	}
	
}
