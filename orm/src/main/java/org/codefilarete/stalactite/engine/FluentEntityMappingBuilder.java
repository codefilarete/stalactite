package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.MapOptions.KeyAsEntityMapOptions;
import org.codefilarete.stalactite.engine.MapOptions.ValueAsEntityMapOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Serie;
import org.codefilarete.tool.function.TriFunction;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Nullable.nullable;

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
	 * By default, the entity no-arg constructor is used to instantiate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given {@link Column}.
	 * By default, the entity no-arg constructor is used to instantiate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, Column<T, I> column);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given column name.
	 * By default, the entity no-arg constructor is used to instantiate them, but you may change this behavior thanks to one of the
	 * {@link FluentEntityMappingBuilderKeyOptions#usingConstructor(Function)} methods
	 *
	 * @param getter getter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableFunction<C, I> getter, IdentifierPolicy<I> identifierPolicy, String columnName);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}.
	 * By default, the entity no-arg constructor is used to instantiate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given {@link Column}.
	 * By default, the entity no-arg constructor is used to instantiate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	<T extends Table> FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, Column<T, I> column);
	
	/**
	 * Maps given property as identifier, using given {@link IdentifierPolicy}, and given column name.
	 * By default, the entity no-arg constructor is used to instantiate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param identifierPolicy {@link IdentifierPolicy} to be used for entity insertion
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderKeyOptions<C, I> mapKey(SerializableBiConsumer<C, I> setter, IdentifierPolicy<I> identifierPolicy, String columnName);
	
	/**
	 * Maps given property as a composite-identifier. The {@link IdentifierPolicy} is already-assigned.
	 * By default, the entity no-arg constructor is used to instantiate them.
	 *
	 * @param getter getter of the property to be used as key
	 * @param compositeKeyMappingBuilder a configuration that details the properties that composes the identifier
	 * @param markAsPersistedFunction the {@link Consumer} that allows to mark the entity as "inserted in database"
	 * @param isPersistedFunction the {@link Function} that allows to know if entity was already inserted in database
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderCompositeKeyOptions<C, I> mapCompositeKey(SerializableFunction<C, I> getter,
																		CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																		Consumer<C> markAsPersistedFunction,
																		Function<C, Boolean> isPersistedFunction);
	
	/**
	 * Maps given property as a composite-identifier. The {@link IdentifierPolicy} is already-assigned.
	 * By default, the entity no-arg constructor is used to instantiate them.
	 *
	 * @param setter setter of the property to be used as key
	 * @param compositeKeyMappingBuilder a configuration that details the properties that composes the identifier
	 * @param markAsPersistedFunction the {@link Consumer} that allows to mark the entity as "inserted in database"
	 * @param isPersistedFunction the {@link Function} that allows to know if entity was already inserted in database
	 * @return an object for configuration chaining
	 */
	FluentEntityMappingBuilderCompositeKeyOptions<C, I> mapCompositeKey(SerializableBiConsumer<C, I> setter,
																		CompositeKeyMappingConfigurationProvider<I> compositeKeyMappingBuilder,
																		Consumer<C> markAsPersistedFunction,
																		Function<C, Boolean> isPersistedFunction);
	
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
		 * @param factory 1-arg constructor to be used
		 * @return this
		 */
		KeyOptions<C, I> usingConstructor(Function<? super I, C> factory);
		
		/**
		 * Indicates the 1-arg constructor to be used to instantiate entity. It will be given column value (expected to be table primary key).
		 * 
		 * @param factory 1-arg constructor to be used
		 * @param input column to use for retrieving value to be given as constructor argument
		 * @return this
		 */
		<T extends Table<T>> KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input);
		
		/**
		 * Variant of {@link #usingConstructor(Function, Column)} with only column name.
		 * 
		 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
		 * @param columnName name of column to use for retrieving value to be given as constructor argument
		 */
		KeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName);
		
		/**
		 * Variant of {@link #usingConstructor(Function, Column)} with 2 {@link Column}s : first one is expected to be primary key,
		 * while second one is an extra data.
		 * About extra data : if it has an equivalent property, then it should be marked as set by constructor to avoid a superfluous
		 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, its mapping declaration ({@link PropertyOptions#column(Column)})
		 * should use this column as argument to make whole mapping consistent. 
		 *
		 * @param factory 2-args constructor to use (can also be a method factory, not a pure class constructor)
		 * @param input1 first column to use for retrieving value to be given as constructor argument
		 * @param input2 second column to use for retrieving value to be given as constructor argument
		 */
		<X, T extends Table<T>> KeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
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
		 * call to its setter, through {@link PropertyOptions#setByConstructor()}. Also, their mapping declarations ({@link PropertyOptions#column(Column)})
		 * should use the columns as argument to make whole mapping consistent. 
		 *
		 * @param factory 3-args constructor to use (can also be a method factory, not a pure class constructor)
		 * @param input1 first column to use for retrieving value to be given as constructor argument
		 * @param input2 second column to use for retrieving value to be given as constructor argument
		 * @param input3 third column to use for retrieving value to be given as constructor argument
		 */
		<X, Y, T extends Table<T>> KeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
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
		 * This method is expected to be used in conjunction with {@link PropertyOptions#setByConstructor()} and {@link PropertyOptions#column(Column)}.
		 *
		 * @param factory the constructor to use (can also be a method factory, not a pure class constructor)
		 */
		// signature note : the generics wildcard ? are actually expected to be of same type, but left it as it is because setting a generics type
		// for it makes usage of Function::apply quite difficult (lot of cast) because the generics type car hardly be something else than Object  
		KeyOptions<C, I> usingFactory(Function<Function<Column<?, ?>, ?>, C> factory);
		
	}
	
	interface CompositeKeyOptions<C, I> {
		
	}
	
	interface FluentEntityMappingBuilderCompositeKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, CompositeKeyOptions<C, I> {
		
	}
	
	interface FluentEntityMappingBuilderKeyOptions<C, I> extends FluentEntityMappingBuilder<C, I>, KeyOptions<C, I> {
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Supplier<C> factory);
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory);
		
		@Override
		<T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, Column<T, I> input);
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(Function<? super I, C> factory, String columnName);
		
		@Override
		<X, T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																							Column<T, I> input1,
																							Column<T, X> input2);
		
		@Override
		<X> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(BiFunction<? super I, X, C> factory,
																		String columnName1,
																		String columnName2);
		
		@Override
		<X, Y, T extends Table<T>> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																							   Column<T, I> input1,
																							   Column<T, X> input2,
																							   Column<T, Y> input3);
		
		@Override
		<X, Y> FluentEntityMappingBuilderKeyOptions<C, I> usingConstructor(TriFunction<? super I, X, Y, C> factory,
																		   String columnName1,
																		   String columnName2,
																		   String columnName3);
		
		@Override
		FluentEntityMappingBuilderKeyOptions<C, I> usingFactory(Function<Function<Column<?, ?>, ?>, C> factory);
	}
	
	<O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableBiConsumer<C, O> setter);
	
	<O> FluentMappingBuilderPropertyOptions<C, I, O> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentMappingBuilderEnumOptions<C, I, E> mapEnum(SerializableFunction<C, E> getter);
	
	<K, V, M extends Map<K, V>> FluentMappingBuilderMapOptions<C, I, K, V, M> mapMap(SerializableFunction<C, M> getter, Class<K> keyType, Class<V> valueType);
	
	<K, V, M extends Map<K, V>> FluentMappingBuilderMapOptions<C, I, K, V, M> mapMap(SerializableBiConsumer<C, M> setter, Class<K> keyType, Class<V> valueType);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableFunction<C, S> getter, Class<O> componentType,
																												   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	<O, S extends Collection<O>> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> mapCollection(SerializableBiConsumer<C, S> setter, Class<O> componentType,
																												   EmbeddableMappingConfigurationProvider<O> embeddableConfiguration);
	
	FluentEntityMappingBuilder<C, I> withTableNaming(TableNamingStrategy tableNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withJoinColumnNaming(JoinColumnNamingStrategy joinColumnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withMapEntryTableNaming(MapEntryTableNamingStrategy tableNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	/**
	 * Declares the mapping of a super class.
	 * As a difference with {@link #mapSuperClass(EmbeddableMappingConfiguration)}, identifier policy must be defined
	 * by given configuration (or the highest ancestor, not intermediary), not by current one : if id policy is
	 * also-or-only defined by the current builder, an exception will be thrown at build time.
	 * This method should be used when given configuration acts as a parent entity, maybe stored on a different table
	 * than current one (see {@link InheritanceOptions#withJoinedTable()}.
	 * Note that for now relations of given configuration are not taken into account (not implemented).
	 * 
	 * @param mappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	default FluentMappingBuilderInheritanceOptions<C, I> mapSuperClass(EntityMappingConfiguration<? super C, I> mappingConfiguration) {
		return this.mapSuperClass(() -> (EntityMappingConfiguration<C, I>) mappingConfiguration);
	}
	
	/**
	 * Declares the mapping of a super class.
	 * As a difference with {@link #mapSuperClass(EmbeddableMappingConfiguration)}, identifier policy must be defined
	 * by given configuration (or the highest ancestor, not intermediary), not by current one : if id policy is
	 * also-or-only defined by the current builder, an exception will be thrown at build time.
	 * This method should be used when given configuration acts as a parent entity, maybe stored on a different table
	 * than current one (see {@link InheritanceOptions#withJoinedTable()}.
	 * Note that for now relations of given configuration are not taken into account (not implemented).
	 *
	 * @param mappingConfigurationProvider a mapping configuration of a super type of the current mapped type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	FluentMappingBuilderInheritanceOptions<C, I> mapSuperClass(EntityMappingConfigurationProvider<? super C, I> mappingConfigurationProvider);
	
	/**
	 * Declares the mapping of a super class.
	 * Id policy must be defined by current configuration.
	 * This method should be used when given configuration is reusable between entities, acting as a common and shared
	 * configuration, with no impact on table or id policy, since table and id policy must be defined by current configuration.
	 * 
	 * @param mappingConfiguration a mapping configuration of a super type of the current mapped type
	 * @return an enhanced version of {@code this} to let caller pursue its configuration
	 */
	default FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfiguration<? super C> mappingConfiguration) {
		return this.mapSuperClass(() -> (EmbeddableMappingConfiguration<C>) mappingConfiguration);
	}
	
	/**
	 * Declares the mapping of a super class.
	 * Id policy must be defined by current configuration.
	 * This method should be used when given configuration is reusable between entities, acting as a common and shared
	 * configuration, with no impact on table or id policy, since table and id policy must be defined by current configuration.
	 *
	 * @param mappingConfigurationProvider a mapping configuration of a super type of the current mapped type
	 * @return an enhanced version of {@code this} to let caller pursue its configuration
	 */
	FluentEntityMappingBuilder<C, I> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> mappingConfigurationProvider);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 * 
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	default <O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableFunction<C, O> getter,
																							 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		return mapOneToOne(getter, mappingConfiguration, null);
	}
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	default <O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																			EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		return mapOneToOne(setter, mappingConfiguration, null);
	}
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableFunction<C, O> getter,
																					 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration,
																					 T table);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, T extends Table> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																					 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration,
																					 T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type. 
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	default <O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToMany(getter, mappingConfiguration, (Table) null);
	}
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param tableName an optional table name to use for target entity on this particular relation
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	default
	<O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable String tableName) {
		return mapOneToMany(getter, mappingConfiguration, nullable(tableName).map(Table::new).get());
	}
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @param <T> table type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Collection<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 * 
	 * @param setter the way to set the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	default <O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapOneToMany(setter, mappingConfiguration, (Table) null);
	}
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param setter the way to set the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param tableName an optional table name to use for target entity on this particular relation
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	default
	<O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable String tableName) {
		return mapOneToMany(setter, mappingConfiguration, nullable(tableName).map(Table::new).get());
	}
	
	/**
	 * Declares a relation between current entity and some of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param setter the way to set the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @param <T> table type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Collection<O>, T extends Table>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a many-to-many relation between current entity and some of type {@code O}.
	 * Depending on collection type, order persistence can be asked by one of the {@link ManyToManyOptions#indexed()}
	 * methods.
	 * For bidirectional relation, you may be interested in using {@link ManyToManyOptions#reverseCollection(SerializableFunction)}
	 * or {@link ManyToManyOptions#reverselySetBy(SerializableBiConsumer)} on returned instance.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	default <O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration) {
		return mapManyToMany(getter, mappingConfiguration, (Table) null);
	}
	
	/**
	 * Declares a many-to-many relation between current entity and some of type {@code O}.
	 * Depending on collection type, order persistence can be asked by one of the {@link ManyToManyOptions#indexed()}
	 * methods.
	 * For bidirectional relation, you may be interested in using {@link ManyToManyOptions#reverseCollection(SerializableFunction)}
	 * or {@link ManyToManyOptions#reverselySetBy(SerializableBiConsumer)} on returned instance.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param tableName an optional table name to use for target entity on this particular relation
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	default
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable String tableName) {
		return mapManyToMany(getter, mappingConfiguration, nullable(tableName).map(Table::new).get());
	}
	
	/**
	 * Declares a many-to-many relation between current entity and some of type {@code O}.
	 * Depending on collection type, order persistence can be asked by one of the {@link ManyToManyOptions#indexed()}
	 * methods.
	 * For bidirectional relation, you may be interested in using {@link ManyToManyOptions#reverseCollection(SerializableFunction)}
	 * or {@link ManyToManyOptions#reverselySetBy(SerializableBiConsumer)} on returned instance.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @param <T> table type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>, T extends Table>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a many-to-many relation between current entity and some of type {@code O}.
	 * Depending on collection type, order persistence can be asked by one of the {@link ManyToManyOptions#indexed()}
	 * methods.
	 * For bidirectional relation, you may be interested in using {@link ManyToManyOptions#reverseCollection(SerializableFunction)}
	 * or {@link ManyToManyOptions#reverselySetBy(SerializableBiConsumer)} on returned instance.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param setter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param tableName an optional table name to use for target entity on this particular relation
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	default
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable String tableName) {
		return mapManyToMany(setter, mappingConfiguration, nullable(tableName).map(Table::new).get());
	}
	
	/**
	 * Declares a many-to-many relation between current entity and some of type {@code O}.
	 * Depending on collection type, order persistence can be asked by one of the {@link ManyToManyOptions#indexed()}
	 * methods.
	 * For bidirectional relation, you may be interested in using {@link ManyToManyOptions#reverseCollection(SerializableFunction)}
	 * or {@link ManyToManyOptions#reverselySetBy(SerializableBiConsumer)} on returned instance.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param setter the way to get the {@link Set} from source entities
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @param <T> table type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>, T extends Table>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration, @javax.annotation.Nullable T table);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	default <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableFunction<C, O> getter,
																										  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		return mapManyToOne(getter, mappingConfiguration, null);
	}
	
	/**
	 * Declares a many-to-one relation between current entity and some of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	default <O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableBiConsumer<C, O> setter,
																										  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration) {
		return mapManyToOne(setter, mappingConfiguration, null);
	}
	
	/**
	 * Declares a many-to-one relation between current entity and some of type {@code O}.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableFunction<C, O> getter,
																								  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration,
																								  Table table);
	
	/**
	 * Declares a many-to-one relation between current entity and some of type {@code O}.
	 * Allows to overwrite the target entity table on this relation : it will overwrite the eventually one of the mapping configuration or the one
	 * deduced from naming strategy. Made for eventual reuse of target entity from persistence context to another.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param table an optional table to use for target entity on this particular relation
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableBiConsumer<C, O> setter,
																								  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration,
																								  Table table);
	
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
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
	
	interface FluentMappingBuilderPropertyOptions<C, I, O>
			extends
			FluentEntityMappingBuilder<C, I>,
			ColumnOptions<O>,
			ExtraTablePropertyOptions {
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> mandatory();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> setByConstructor();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> readonly();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> columnName(String name);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> column(Column<? extends Table, ? extends O> column);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> fieldName(String name);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> extraTableName(String name);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> readConverter(Converter<O, O> converter);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> writeConverter(Converter<O, O> converter);
		
		@Override
		<V> FluentMappingBuilderPropertyOptions<C, I, O> sqlBinder(ParameterBinder<V> parameterBinder);
	}
	
	interface FluentMappingBuilderOneToOneOptions<C, I, O> extends FluentEntityMappingBuilder<C, I>,
			OneToOneOptions<C, I, O> {
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> mandatory();
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(SerializableBiConsumer<? super O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (getter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(SerializableFunction<? super O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(Column<?, I> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseColumnName opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> mappedBy(String reverseColumnName);

		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param relationMode any {@link RelationMode}
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> cascading(RelationMode relationMode);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToOneOptions<C, I, O> fetchSeparately();
	}
	
	interface FluentMappingBuilderOneToManyOptions<C, I, O, S extends Collection<O>> extends FluentEntityMappingBuilder<C, I>, OneToManyOptions<C, I, O, S> {
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableBiConsumer<O, ? super C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(SerializableFunction<O, ? super C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation (setter)
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(Column<?, I> reverseLink);
		
		/**
		 * 
		 * @param reverseColumnName opposite owner of the relation
		 * @return
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> mappedBy(String reverseColumnName);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param collectionFactory a collection factory
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> initializeWith(Supplier<S> collectionFactory);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param relationMode any {@link RelationMode}
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> cascading(RelationMode relationMode);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> fetchSeparately();
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 * 
		 * @param orderingColumn orderingColumn the column to be used for order persistence
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(Column<?, Integer> orderingColumn);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @param columnName the column name to be used for order persistence
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexedBy(String columnName);
		
		/**
		 * {@inheritDoc}
		 * Declaration overridden to adapt return type to this class.
		 *
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderOneToManyOptions<C, I, O, S> indexed();
		
	}
	
	interface FluentMappingBuilderManyToManyOptions<C, I, O, S1 extends Collection<O>, S2 extends Collection<C>> extends FluentEntityMappingBuilder<C, I>, ManyToManyOptions<C, I, O, S1, S2> {
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> initializeWith(Supplier<S1> collectionFactory);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableFunction<O, S2> collectionAccessor);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverseCollection(SerializableBiConsumer<O, S2> collectionMutator);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> reverselyInitializeWith(Supplier<S2> collectionFactory);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> cascading(RelationMode relationMode);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> fetchSeparately();
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexedBy(String columnName);
		
		@Override
		FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2> indexed();
		
	}
	
	interface FluentMappingBuilderManyToOneOptions<C, I, O, S extends Collection<C>> extends FluentEntityMappingBuilder<C, I>,
			ManyToOneOptions<C, I, O, S> {
		
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> mandatory();
		
		/**
		 * Defines combiner of current entity with target entity. This is a more fine-grained way to define how to combine current entity with target
		 * entity than {@link #reverseCollection(SerializableFunction)} : sometimes a method already exists in entities to fill the relation instead of
		 * calling getter + Collection.add. This method is here to benefit from it.
		 * This method has no consequence on database mapping since it only interacts in memory.
		 *
		 * @param reverseLink opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O,S> reverselySetBy(SerializableBiConsumer<O, C> reverseLink);
		
		/**
		 * Defines reverse collection accessor.
		 * Used to fill an (in-memory) bi-directionality.
		 * This method has no consequence on database mapping since it only interacts in memory.
		 *
		 * @param collectionAccessor opposite owner of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> reverseCollection(SerializableFunction<O, S> collectionAccessor);
		
		/**
		 * Defines reverse collection mutator.
		 * Used to fill an (in-memory) bi-directionality.
		 * This method has no consequence on database mapping since it only interacts in memory.
		 *
		 * @param collectionMutator opposite setter of the relation
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> reverseCollection(SerializableBiConsumer<O, S> collectionMutator);
		
		/**
		 * Defines the factory of reverse collection. If not defined and collection is found null, the collection is set with a default value.
		 * This method has no consequence on database mapping since it only interacts in memory.
		 *
		 * @param collectionFactory opposite collection factory
		 * @return the global mapping configurer
		 */
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> reverselyInitializeWith(Supplier<S> collectionFactory);
		
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> cascading(RelationMode relationMode);
		
		@Override
		FluentMappingBuilderManyToOneOptions<C, I, O, S> fetchSeparately();
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
	
	interface FluentMappingBuilderEnumOptions<C, I, E extends Enum<E>>
			extends FluentEntityMappingBuilder<C, I>, EnumOptions<E> {
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> byName();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> byOrdinal();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> mandatory();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> readonly();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> columnName(String name);
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> column(Column<? extends Table, ? extends E> column);
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> fieldName(String name);
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> readConverter(Converter<E, E> converter);
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> writeConverter(Converter<E, E> converter);
		
		@Override
		<V> FluentMappingBuilderEnumOptions<C, I, E> sqlBinder(ParameterBinder<V> parameterBinder);
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
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withReverseJoinColumn(String name);
		
		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(Table table);

		@Override
		FluentMappingBuilderElementCollectionOptions<C, I, O, S> withTable(String tableName);
		
	}
	
	interface FluentMappingBuilderMapOptions<C, I, K, V, M extends Map<K, V>>
			extends FluentEntityMappingBuilder<C, I>, MapOptions<K, V, M> {
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withReverseJoinColumn(String columnName);
		
		FluentMappingBuilderMapOptions<C, I, K, V, M> withKeyColumn(String columnName);
		
		FluentMappingBuilderMapOptions<C, I, K, V, M> withValueColumn(String columnName);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withMapFactory(Supplier<? extends M> collectionFactory);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withTable(String tableName);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withTable(Table table);
		
		@Override
		FluentMappingBuilderKeyAsEntityMapOptions<C, I, K, V, M> withKeyMapping(EntityMappingConfigurationProvider<K, ?> mappingConfigurationProvider);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider);
		
		@Override
		FluentMappingBuilderValueAsEntityMapOptions<C, I, K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> withValueMapping(EmbeddableMappingConfigurationProvider<V> mappingConfigurationProvider);
		
		@Override
		<IN> FluentMappingBuilderMapOptions<C, I, K, V, M> overrideKeyColumnName(SerializableFunction<K, IN> getter, String columnName);
		
		@Override
		<IN> FluentMappingBuilderMapOptions<C, I, K, V, M> overrideKeyColumnName(SerializableBiConsumer<K, IN> setter, String columnName);
		
		@Override
		<IN> FluentMappingBuilderMapOptions<C, I, K, V, M> overrideValueColumnName(SerializableFunction<K, IN> getter, String columnName);
		
		@Override
		<IN> FluentMappingBuilderMapOptions<C, I, K, V, M> overrideValueColumnName(SerializableBiConsumer<K, IN> setter, String columnName);
		
		@Override
		FluentMappingBuilderMapOptions<C, I, K, V, M> fetchSeparately();
	}
	
	interface FluentMappingBuilderKeyAsEntityMapOptions<C, I, K, V, M extends Map<K, V>>
			extends FluentMappingBuilderMapOptions<C, I, K, V, M>, KeyAsEntityMapOptions<K, V, M> {
		
		@Override
		FluentMappingBuilderKeyAsEntityMapOptions<C, I, K, V, M> cascading(RelationMode relationMode);
	}
	
	interface FluentMappingBuilderValueAsEntityMapOptions<C, I, K, V, M extends Map<K, V>>
			extends FluentMappingBuilderMapOptions<C, I, K, V, M>, ValueAsEntityMapOptions<K, V, M> {
		
		@Override
		FluentMappingBuilderValueAsEntityMapOptions<C, I, K, V, M> cascading(RelationMode relationMode);
	}
	
	interface FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S extends Collection<O>>
			extends FluentEntityMappingBuilder<C, I>, ElementCollectionOptions<C, O, S> {
		
		<IN> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		<IN> FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withReverseJoinColumn(String name);
		
		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(Table table);

		@Override
		FluentMappingBuilderElementCollectionImportEmbedOptions<C, I, O, S> withTable(String tableName);
	}
}
