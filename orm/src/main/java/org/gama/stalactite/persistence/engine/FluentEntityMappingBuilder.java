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
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class. 
 * Please note that it can't extend {@link FluentEmbeddableMappingBuilder} because it clashes on the {@link #build(PersistenceContext)} methods that don't
 * have compatible return type.
 * 
 * @author Guillaume Mary
 * @see MappingEase#entityBuilder(Class, Class)
 * @see #build(PersistenceContext)
 */
public interface FluentEntityMappingBuilder<C, I> extends PersisterBuilder<C, I>, EntityMappingConfigurationProvider<C, I> {
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}.
	 * By default, the entity no-arg constructor is used to instanciate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy identifierPolicy);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given {@link Column}.
	 * By default, the entity no-arg constructor is used to instanciate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy identifierPolicy, Column<T, I> column);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given column name.
	 * By default, the entity no-arg constructor is used to instanciate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy identifierPolicy, String columnName);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}.
	 * By default, the entity no-arg constructor is used to instanciate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy identifierPolicy);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given {@link Column}.
	 * By default, the entity no-arg constructor is used to instanciate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy identifierPolicy, Column<T, I> column);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given column name.
	 * By default, the entity no-arg constructor is used to instanciate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy identifierPolicy, String columnName);
	
	/**
	 * Interface for {@link #mapKey(SerializableFunction, IdentifierPolicy)} family methods return. Aimed at chaining to configure entity key mapping. 
	 * @param <C> entity type
	 * @param <I> identifier type
	 */
	interface KeyOptions<C, I> {
		
		/**
		 * Indicates a no-arg factory to be used to instantiate entity.
		 * 
		 * @param factory any no-arg method returning an instance of C
		 * @return this
		 */
		KeyOptions<C, I> usingConstructor(Supplier<C> factory);
		
		/**
		 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given primary key value.
		 * 
		 * @param factory 1-arg contructor to be used
		 * @return this
		 */
		KeyOptions<C, I> usingConstructor(Function<? super I, C> factory);
		
		/**
		 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given column value (expected to be table primary key).
		 * 
		 * @param factory 1-arg contructor to be used
		 * @param input column to use for retrieving value to be given as contructor argument
		 * @return this
		 */
		<T extends Table> KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input);
		
		/**
		 * Variant of {@link #usingConstructor(Function, Column)} with only column name.
		 * 
		 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
		 * @param columnName name of column to use for retrieving value to be given as contructor argument
		 */
		KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName);
		
		/**
		 * Variant of {@link #usingConstructor(Function, Column)} with 2 {@link Column}s : first one is expected to be primary key,
		 * while second one is an extra data.
		 * About extra data : if it has an equivalent property, then it should be marked as set by constructor to avoid a superfluous
		 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, its mapping declaration ({@link #add(SerializableFunction, Column)})
		 * should use this column as argument to make whole mapping consistent. 
		 *
		 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
		 * @param input1 first column to use for retrieving value to be given as contructor argument
		 * @param input2 second column to use for retrieving value to be given as contructor argument
		 */
		<X, T extends Table> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
															   Column<T, I> input1,
															   Column<T, X> input2);
		
		/**
		 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with only column names.
		 *
		 * @param factory constructor to use (can also be a method factory, not a pure class constructor)
		 * @param columnName1 first column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
		 * @param columnName2 second column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
		 */
		<X> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
											  String columnName1,
											  String columnName2);
		
		/**
		 * Variant of {@link #usingConstructor(BiFunction, Column, Column)} with 3 {@link Column}s : first one is expected to be primary key,
		 * while second and third one are extra data.
		 * About extra data : if they have equivalent properties, then they should be marked as set by constructor to avoid a superfluous
		 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, their mapping declarations ({@link #add(SerializableFunction, Column)})
		 * should use the columns as argument to make whole mapping consistent. 
		 *
		 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
		 * @param input1 first column to use for retrieving value to be given as contructor argument
		 * @param input2 second column to use for retrieving value to be given as contructor argument
		 * @param input3 third column to use for retrieving value to be given as contructor argument
		 */
		<X, Y, T extends Table> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																  Column<T, I> input1,
																  Column<T, X> input2,
																  Column<T, Y> input3);
		
		/**
		 * Variant of {@link #usingConstructor(TriFunction, Column, Column, Column)} with only column names.
		 *
		 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
		 * @param columnName1 first column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
		 * @param columnName2 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
		 * @param columnName3 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
		 */
		<X, Y> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
												 String columnName1,
												 String columnName2,
												 String columnName3);
		
		/**
		 * Very open variant of {@link #usingConstructor(Function)} that gives a {@link Function} to be used to create instances.
		 * 
		 * Given factory gets a <pre>Function<? extends Column, ? extends Object></pre> as unique argument (be aware that as a consequence its
		 * code will depend on {@link Column}) which represent a kind of {@link java.sql.ResultSet} so one can fulfill any property of its instance
		 * the way he wants.
		 * <br/>
		 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} and {@link #add(SerializableFunction, Column)}.
		 *
		 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
		 */
		// signature note : the generics wildcard ? are actually expected to be of same type, but left it as it is because setting a generics type
		// for it makes usage of Function::apply quite difficult (lot of cast) because the generics type car hardly be something else than Object  
		<T extends Table> KeyOptions<C, I> usingFactory(Function<Function<Column<T, ?>, ?>, C> factory);
		
	}
	
	interface FluentEntityMappingBuilderKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, KeyOptions<C, I> {
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Supplier<C> factory);
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory);
		
		@Override
		<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input);
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName);
		
		@Override
		<X, T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																						 Column<T, I> input1,
																						 Column<T, X> input2);
		
		@Override
		<X> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																		String columnName1,
																		String columnName2);
		
		@Override
		<X, Y, T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																							Column<T, I> input1,
																							Column<T, X> input2,
																							Column<T, Y> input3);
		
		@Override
		<X, Y> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																		   String columnName1,
																		   String columnName2,
																		   String columnName3);
		
		@Override
		<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> usingFactory(Function<Function<Column<T, ?>, ?>, C> factory);
	}
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter);
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter);
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, String columnName);
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableBiConsumer<C, O> setter, Column<? extends Table, O> column);
	
	<O> FluentMappingBuilderPropertyOptions<C, I> add(SerializableFunction<C, O> getter, Column<? extends Table, O> column);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableBiConsumer<C, E> setter, Column<? extends Table, E> column);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I> addEnum(SerializableFunction<C, E> getter, Column<? extends Table, E> column);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableFunction<C, S> getter, Class<O> componentType,
																												   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> addCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType,
																												   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	FluentEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy);
	
	/**
	 * Indicates a constructor to use instead of a default no-arg one (for Select feature).
	 * Can be necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * <br/>
	 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 * 
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input the column to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param <X> constructor first argument type
	 */
	<X, T extends Table<T>> FluentEntityMappingBuilder<C, I> useConstructor(Function<X, C> factory, Column<T, X> input);
	
	/**
	 * Indicates a constructor to use instead of a default no-arg one (for Select feature).
	 * Can be necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * <br/>
	 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param input1 the column to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param input2 the column to read in {@link java.sql.ResultSet} and make its value given as factory second argument
	 * @param <X> constructor first argument type
	 * @param <Y> constructor second argument type
	 */
	<X, Y, T extends Table<T>> FluentEntityMappingBuilder<C, I> useConstructor(BiFunction<X, Y, C> factory,
																			   Column<T, X> input1,
																			   Column<T, Y> input2);

	/**
	 * Indicates a constructor to use instead of a default no-arg one (for Select feature).
	 * Can be necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
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
	<X, Y, Z, T extends Table<T>> FluentEntityMappingBuilder<C, I> useConstructor(TriFunction<X, Y, Z, C> factory,
																				  Column<T, X> input1,
																				  Column<T, Y> input2,
																				  Column<T, Z> input3);

	/**
	 * Variant of {@link #useConstructor(Function, Column)} with only column name.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param <X> constructor first argument type
	 */
	<X> FluentEntityMappingBuilder<C, I> useConstructor(Function<X, C> factory, String columnName);
	
	/**
	 * Variant of {@link #useConstructor(BiFunction, Column, Column)} with only column names.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName2 column name to read in {@link java.sql.ResultSet} and make its value given as factory second argument
	 * @param <X> constructor first argument type
	 */
	<X, Y> FluentEntityMappingBuilder<C, I> useConstructor(BiFunction<X, Y, C> factory, String columnName1, String columnName2);
	
	/**
	 * Variant of {@link #useConstructor(TriFunction, Column, Column, Column)} with only column names.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 * @param columnName1 column name to read in {@link java.sql.ResultSet} and make its value given as factory first argument
	 * @param columnName2 column name to read in {@link java.sql.ResultSet} and make its value given as factory second argument
	 * @param columnName3 column name to read in {@link java.sql.ResultSet} and make its value given as factory third argument
	 * @param <X> constructor first argument type
	 */
	<X, Y, Z> FluentEntityMappingBuilder<C, I> useConstructor(TriFunction<X, Y, Z, C> factory,
															  String columnName1,
															  String columnName2,
															  String columnName3);
	
	/**
	 * Indicates a constructor to use instead of a default no-arg one (for Select feature).
	 * Can be necessary for some entities that doesn't expose their default constructor to fulfill some business rules.
	 * This method is an extended version of {@link #useConstructor(TriFunction, Column, Column, Column)}
	 * where factory must accept a <pre>Function<? extends Column, ? extends Object></pre> as unique argument (be aware that as a consequence its
	 * code will depend on {@link Column}) which represent a kind of {@link java.sql.ResultSet} so one can fulfill any property of its instance
	 * as one wants.
	 * <br/>
	 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} which declares
	 * that the property is already set by constructor so there's no reason to set it again.
	 *
	 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
	 */
	<T extends Table<T>, O> FluentEntityMappingBuilder<C, I> useConstructor(Function<Function<Column<T, O>, O>, C> factory);
	
	/**
	 * Declares the inherited mapping.
	 * Id policy must be defined in the given strategy, not by current configuration : if id policy is also / only defined by the current builder,
	 * an exception will be thrown at build time.
	 * 
	 * @param mappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	FluentMappingBuilderInheritanceOptions<C, I> mapInheritance(EntityMappingConfiguration<? super C, I> mappingConfiguration);
	
	default FluentMappingBuilderInheritanceOptions<C, I> mapInheritance(EntityMappingConfigurationProvider<? super C, I> mappingConfigurationProvider) {
		return this.mapInheritance(mappingConfigurationProvider.getConfiguration());
	}
	
	/**
	 * Declares the mapping of a super class.
	 * 
	 * @param superMappingConfigurationProvider a mapping configuration of a super type of the current mapped type
	 * @return a enhanced version of {@code this} so one can add set options to the relationship or add mapping to {@code this}
	 */
	FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfigurationProvider);
	
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
	 * Declares a relation between current entity and some of type {@code O} through a {@link Set}.
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
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration);
	
	<O, J, S extends Set<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	<O, J, S extends Set<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	addOneToManySet(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link List}.
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
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	FluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withJoinColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Sets {@link ColumnNamingStrategy} for index column of one-to-many {@link List} association
	 * @param columnNamingStrategy maybe null, {@link ColumnNamingStrategy#INDEX_DEFAULT} will be used instead
	 * @return this
	 */
	FluentEntityMappingBuilder<C, I> withIndexColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy);
	
	<V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter);
	
	<V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> sequence);
	
	FluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy);
	
	interface FluentMappingBuilderPropertyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, ColumnOptions<C, I> {
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I> mandatory();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I> setByConstructor();
	}
	
	interface FluentMappingBuilderOneToOneOptions<C, I, T extends Table> extends FluentEntityMappingBuilder<C, I>,
			OneToOneOptions<C, I, T> {
		
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, T> mandatory();
		
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
	
	interface FluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends FluentEntityMappingBuilder<C, I>, OneToManyOptions<C, I, O, S> {
		
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
			extends FluentEntityMappingBuilder<C, I>, ImportedEmbedWithColumnOptions<O> {

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
	
	interface FluentMappingBuilderEnumOptions<C, I>
			extends FluentEntityMappingBuilder<C, I>, EnumOptions {
		
		@Override
		FluentMappingBuilderEnumOptions<C, I> byName();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I> byOrdinal();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I> mandatory();
	}
	
	interface FluentMappingBuilderInheritanceOptions<C, I>
			extends FluentEntityMappingBuilder<C, I>, InheritanceOptions {
		
		@Override
		FluentMappingBuilderInheritanceOptions<C, I> withJoinedTable();
		
		@Override
		FluentMappingBuilderInheritanceOptions<C, I> withJoinedTable(Table parentTable);
		
	}
	
	interface FluentMappingBuilderElementCollectionOptions<C, I, O, S extends Collection<O>>
			extends FluentEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> override(String columnName);

		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(Table table);

		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(String tableName);
	}
	
	interface FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S extends Collection<O>>
			extends FluentEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		<IN> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		<IN> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mappedBy(String name);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(Table table);

		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(String tableName);
	}
}
