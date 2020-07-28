package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.function.Serie;
import org.gama.lang.function.TriFunction;
import org.gama.reflection.AccessorChain;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class. 
 * Please note that it can't extend {@link IFluentEmbeddableMappingBuilder} because it clashes on the {@link #build(PersistenceContext)} methods that don't
 * have compatible return type.
 * 
 * @author Guillaume Mary
 * @see MappingEase#entityBuilder(Class, Class)
 * @see #build(PersistenceContext)
 */
public interface IFluentEntityMappingBuilder<C, I> extends IFluentEmbeddableMappingConfiguration<C>, PersisterBuilder<C, I>, EntityMappingConfigurationProvider<C, I> {
	
	/* Overwritting methods signature to return a type that aggregates options of this class */
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter);
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName);
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column);
	
	<O> IFluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<? extends Table, O> column);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column);
	
	<E extends Enum<E>> IFluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column);
	
	<O, S extends Collection<O>> IFluentMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType);
	
	<O, S extends Collection<O>> IFluentMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType);
	
	<O, S extends Collection<O>> IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType,
																										 EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	<O, S extends Collection<O>> IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType,
																										 EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	IFluentEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	IFluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy);
	
	/**
	 * Indicates a constructor to used instead of a default no-arg one (for Select feature).
	 * Can by necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * <br/><br/>
	 * this method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 * 
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input the column to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param <X> constructor first argument type
	 */
	<X> IFluentEntityMappingBuilder<C, I> useConstructor(Function<X, C> factory, Column<? extends Table, X> input);
	
	/**
	 * Indicates a constructor to used instead of a default no-arg one (for Select feature).
	 * Can by necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * <br/><br/>
	 * this method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 the column to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param input2 the column to read in {@link java.sql.ResultSet} and make its value given as factory second argument
	 * @param <X> constructor first argument type
	 * @param <Y> constructor second argument type
	 */
	<X, Y> IFluentEntityMappingBuilder<C, I> useConstructor(BiFunction<X, Y, C> factory,
															Column<? extends Table, X> input1,
															Column<? extends Table, Y> input2);
	
	/**
	 * Indicates a constructor to used instead of a default no-arg one (for Select feature).
	 * Can by necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * <br/><br/>
	 * this method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 the column to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param input2 the column to read in {@link java.sql.ResultSet} and make its value given as factory second argument
	 * @param input3 the column to read in {@link java.sql.ResultSet} and make its value given as factory third argument
	 * @param <X> constructor first argument type
	 * @param <Y> constructor second argument type
	 * @param <Z> constructor third argument type
	 */
	<X, Y, Z> IFluentEntityMappingBuilder<C, I> useConstructor(TriFunction<X, Y, Z, C> factory,
															   Column<? extends Table, X> input1,
															   Column<? extends Table, Y> input2,
															   Column<? extends Table, Z> input3);
	
	/**
	 * Indicates a constructor to used instead of a default no-arg one (for Select feature).
	 * Can by necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * This method is an extended version of {@link #useConstructor(TriFunction, Column, Column, Column)}
	 * where factory must accept a <pre>Function<? extends Column, ? extends Object></pre> as unique argument (be aware that as a consequence its
	 * code will depend on {@link Column}) which represent a kind of {@link java.sql.ResultSet} so one can fulfill any property of its instance
	 * as one wants.
	 * <br/><br/>
	 * this method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 */
	IFluentEntityMappingBuilder<C, I> useConstructor(Function<? extends Function<? extends Column, ? extends Object>, C> factory);
	
	/**
	 * Declares the inherited mapping.
	 * Id policy must be defined in the given strategy, not by current configuration : if id policy is also / only defined by the current builder,
	 * an exception will be thrown at build time.
	 * 
	 * @param mappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	IFluentMappingBuilderInheritanceOptions<C, I> mapInheritance(EntityMappingConfiguration<? super C, I> mappingConfiguration);
	
	default IFluentMappingBuilderInheritanceOptions<C, I> mapInheritance(EntityMappingConfigurationProvider<? super C, I> mappingConfigurationProvider) {
		return this.mapInheritance(mappingConfigurationProvider.getConfiguration());
	}
	
	/**
	 * Declares the mapping of a super class.
	 * 
	 * @param superMappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	IFluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration);
	
	default IFluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfigurationProvider) {
		return this.mapSuperClass(superMappingConfigurationProvider.getConfiguration());
	}
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 * 
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relationship between current entity and some of type {@code O}.
	 *
	 * @param setter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return a enhanced version of {@code this} so one can add options to the relationship or add mapping to {@code this}
	 */
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
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
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableFunction<C, O> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
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
	<O, J, T extends Table> IFluentMappingBuilderOneToOneOptions<C, I, T> addOneToOne(SerializableBiConsumer<C, O> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, T table);
	
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
	IFluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	<O, J, S extends List<O>, T extends Table>
	IFluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends List<O>, T extends Table>
	IFluentMappingBuilderOneToManyListOptions<C, I, O, S>
	addOneToManyList(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter);
	
	@Override
	<O> IFluentMappingBuilderEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter);
	
	@Override
	<O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	@Override
	<O> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	IFluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	IFluentEntityMappingBuilder<C, I> withJoinColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Sets {@link ColumnNamingStrategy} for index column of one-to-many {@link List} association
	 * @param columnNamingStrategy maybe null, {@link ColumnNamingStrategy#INDEX_DEFAULT} will be used instead
	 * @return this
	 */
	IFluentEntityMappingBuilder<C, I> withIndexColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	IFluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy);
	
	<V> IFluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter);
	
	<V> IFluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> sequence);
	
	IFluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy polymorphismPolicy);
	
	interface IFluentMappingBuilderPropertyOptions<C, I> extends IFluentEntityMappingBuilder<C, I>, IFluentEmbeddableMappingConfigurationPropertyOptions<C>, ColumnOptions<C, I> {
		
		@Override
		IFluentMappingBuilderPropertyOptions<C, I> identifier(IdentifierPolicy identifierPolicy);
		
		@Override
		IFluentMappingBuilderPropertyOptions<C, I> mandatory();
		
		@Override
		IFluentMappingBuilderPropertyOptions<C, I> setByConstructor();
	}
	
	interface IFluentMappingBuilderOneToOneOptions<C, I, T extends Table> extends IFluentEntityMappingBuilder<C, I>,
			OneToOneOptions<C, I, T> {
		
		@Override
		IFluentMappingBuilderOneToOneOptions<C, I, T> mandatory();
		
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
		
		@Override
		IFluentMappingBuilderOneToOneOptions<C, I, T> cascading(RelationMode relationMode);
	}
	
	interface IFluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends IFluentEntityMappingBuilder<C, I>, OneToManyOptions<C, I, O, S> {
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink);
		
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
		
		@Override
		IFluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode);
		
	}
	
	/**
	 * A merge of {@link IFluentMappingBuilderOneToManyOptions} and {@link IndexableCollectionOptions} to defined a one-to-many relation
	 * with a indexed {@link java.util.Collection} such as a {@link List}
	 * 
	 * @param <C> type of source entity
	 * @param <I> type of identifier of source entity
	 * @param <O> type of target entities
	 */
	interface IFluentMappingBuilderOneToManyListOptions<C, I, O, S extends List<O>>
			extends IFluentMappingBuilderOneToManyOptions<C, I, O, S>, IndexableCollectionOptions<C, I, O> {
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * Declaration overriden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> mappedBy(Column<Table, ?> reverseLink);
		
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
		
		/**
		 * Defines the indexing column of the mapped {@link java.util.List}.
		 * @param orderingColumn indexing column of the mapped {@link java.util.List}
		 * @return the global mapping configurer
		 */
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
		
		@Override
		IFluentMappingBuilderOneToManyListOptions<C, I, O, S> cascading(RelationMode relationMode);
	}
	
	interface IFluentMappingBuilderEmbedOptions<C, I, O>
			extends IFluentEntityMappingBuilder<C, I>, IFluentEmbeddableMappingConfigurationEmbedOptions<C, O>, EmbedWithColumnOptions<O> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param getter the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		/**
		 * Overrides embedding with an existing target column
		 *
		 * @param function the getter as a method reference
		 * @param targetColumn a column that's the target of the getter
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, O> override(SerializableFunction<O, IN> function, Column<? extends Table, IN> targetColumn);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> IFluentMappingBuilderEmbedOptions<C, I, IN> innerEmbed(SerializableBiConsumer<O, IN> setter);
		
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
	interface IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O>
			extends IFluentEntityMappingBuilder<C, I>, IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O>, ImportedEmbedWithColumnOptions<O> {

		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideName(SerializableFunction<O, IN> function, String columnName);

		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> override(SerializableFunction<O, IN> function, Column<? extends Table, IN> targetColumn);
		
		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> exclude(SerializableBiConsumer<O, IN> setter);

		@Override
		<IN> IFluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> exclude(SerializableFunction<O, IN> getter);
	}
	
	interface IFluentMappingBuilderEnumOptions<C, I>
			extends IFluentEntityMappingBuilder<C, I>, IFluentEmbeddableMappingConfigurationEnumOptions<C> {
		
		@Override
		IFluentMappingBuilderEnumOptions<C, I> byName();
		
		@Override
		IFluentMappingBuilderEnumOptions<C, I> byOrdinal();
		
		@Override
		IFluentMappingBuilderEnumOptions<C, I> mandatory();
	}
	
	interface IFluentMappingBuilderInheritanceOptions<C, I>
			extends IFluentEntityMappingBuilder<C, I>, InheritanceOptions {
		
		@Override
		IFluentMappingBuilderInheritanceOptions<C, I> withJoinedTable();
		
		@Override
		IFluentMappingBuilderInheritanceOptions<C, I> withJoinedTable(Table parentTable);
		
	}
	
	interface IFluentMappingBuilderElementCollectionOptions<C, I, O, S extends Collection<O>>
			extends IFluentEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		@Override
		IFluentMappingBuilderElementCollectionOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		IFluentMappingBuilderElementCollectionOptions<C, I, O, S> override(String columnName);

		@Override
		IFluentMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		IFluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(Table table);

		@Override
		IFluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(String tableName);
	}
	
	interface IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S extends Collection<O>>
			extends IFluentEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		<IN> IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		<IN> IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		@Override
		IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		@Override
		IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(Table table);

		@Override
		IFluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(String tableName);
	}
}
