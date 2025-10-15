package org.codefilarete.stalactite.dsl.entity;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.ExtraTablePropertyOptions;
import org.codefilarete.stalactite.dsl.InheritanceOptions;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.PersisterBuilder;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.embeddable.ImportedEmbedWithColumnOptions;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderCompositeKeyOptions;
import org.codefilarete.stalactite.dsl.key.FluentEntityMappingBuilderKeyOptions;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.property.ColumnOptions;
import org.codefilarete.stalactite.dsl.property.ElementCollectionOptions;
import org.codefilarete.stalactite.dsl.property.EnumOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions.KeyAsEntityMapOptions;
import org.codefilarete.stalactite.dsl.property.MapOptions.ValueAsEntityMapOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.Serie;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

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
	
	FluentEntityMappingBuilder<C, I> withIndexNaming(IndexNamingStrategy indexNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withJoinColumnNaming(JoinColumnNamingStrategy joinColumnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withElementCollectionTableNaming(ElementCollectionTableNamingStrategy tableNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withMapEntryTableNaming(MapEntryTableNamingStrategy tableNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withForeignKeyNaming(ForeignKeyNamingStrategy foreignKeyNamingStrategy);
	
	/**
	 * Sets {@link ColumnNamingStrategy} for index column of one-to-many {@link List} association
	 * @param columnNamingStrategy maybe null, {@link ColumnNamingStrategy#INDEX_DEFAULT} will be used instead
	 * @return this
	 */
	FluentEntityMappingBuilder<C, I> withIndexColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	FluentEntityMappingBuilder<C, I> withAssociationTableNaming(AssociationTableNamingStrategy associationTableNamingStrategy);
	
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
	<O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableFunction<C, O> getter,
																	EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J> FluentMappingBuilderOneToOneOptions<C, I, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																	EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
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
	<O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
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
	<O, J, S extends Collection<O>>
	FluentMappingBuilderOneToManyOptions<C, I, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
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
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
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
	 * @param <O> type of {@link Set} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S1> refined source {@link Collection} type
	 * @param <S2> refined reverse side {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 */
	<O, J, S1 extends Collection<O>, S2 extends Collection<C>>
	FluentMappingBuilderManyToManyOptions<C, I, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relation between current entity and some of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableFunction<C, O> getter,
																								  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a many-to-one relation between current entity and some of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>> FluentMappingBuilderManyToOneOptions<C, I, O, S> mapManyToOne(SerializableBiConsumer<C, O> setter,
																								  EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableFunction<C, O> getter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> embed(SerializableBiConsumer<C, O> setter, EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter);
	
	<V> FluentEntityMappingBuilder<C, I> versionedBy(SerializableFunction<C, V> getter, Serie<V> sequence);
	
	default FluentEntityMappingBuilder<C, I> onTable(String tableName) {
		return this.onTable(new Table(tableName));
	}
	
	FluentEntityMappingBuilder<C, I> onTable(Table table);
	
	FluentEntityMappingBuilder<C, I> mapPolymorphism(PolymorphismPolicy<C> polymorphismPolicy);
	
	interface FluentMappingBuilderPropertyOptions<C, I, O>
			extends
			FluentEntityMappingBuilder<C, I>,
			ColumnOptions<O>,
			ExtraTablePropertyOptions {
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> mandatory();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> unique();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> setByConstructor();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> readonly();
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> columnName(String name);
		
		@Override
		FluentMappingBuilderPropertyOptions<C, I, O> columnSize(Size size);
		
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
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideSize(SerializableFunction<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, I, O> overrideSize(SerializableBiConsumer<O, IN> function, Size columnSize);
		
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
		FluentMappingBuilderEnumOptions<C, I, E> unique();

		@Override
		FluentMappingBuilderEnumOptions<C, I, E> readonly();
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> columnName(String name);
		
		@Override
		FluentMappingBuilderEnumOptions<C, I, E> columnSize(Size size);
		
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
