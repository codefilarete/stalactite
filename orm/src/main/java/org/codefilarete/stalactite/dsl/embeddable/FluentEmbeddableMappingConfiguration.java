package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.property.EnumOptions;
import org.codefilarete.stalactite.dsl.property.PropertyOptions;
import org.codefilarete.stalactite.dsl.relation.ManyToManyOptions;
import org.codefilarete.stalactite.dsl.relation.OneToManyOptions;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class as an embedded bean.
 *
 * @param <C> type that owns method  
 * 
 * @author Guillaume Mary
 * @see FluentEmbeddableMappingBuilder
 */
public interface FluentEmbeddableMappingConfiguration<C> {
	
	/**
	 * Adds a property to be mapped.
	 * By default column name will be extracted from setter according to the Java Bean convention naming.
	 * 
	 * @param setter a Method Reference to a setter
	 * @param <O> setter return type / property type to be mapped
	 * @return this
	 * @see #withColumnNaming(ColumnNamingStrategy)
	 */
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C, O> map(SerializableBiConsumer<C, O> setter);
	
	/**
	 * Adds a property to be mapped. Column name will be extracted from getter according to the Java Bean convention naming.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 * @see #withColumnNaming(ColumnNamingStrategy)
	 */
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C, O> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C, E> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C, E> mapEnum(SerializableFunction<C, E> getter);
	
	/**
	 * Please note that we can't create a generic type for {@code ? super C} by prefixing the method signature with {@code <X super C>}
	 * because it is not syntactically valid (in Java 8). So it can't be shared between the 2 arguments {@code superType} and
	 * {@code mappingStrategy}. So user must be careful to ensure by himself that both types are equal.
	 */
	FluentEmbeddableMappingConfiguration<C> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	/**
	 * Declares a composition between current entity and some embeddable object of type {@code O}.
	 *
	 * @param getter the way to get the embedded bean from this entity
	 * @param embeddableMappingBuilder the persistence configuration of the target embeddable bean
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 * @param <O> embedded bean type
	 */
	<O> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																			 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	/**
	 * Declares a composition between current entity and some embeddable object of type {@code O}.
	 *
	 * @param setter the way to set the embedded bean on this entity
	 * @param embeddableMappingBuilder the persistence configuration of the target embeddable bean
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 * @param <O> embedded bean type
	 */
	<O> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																			 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	/**
	 * Declares a direct relation between current embeddable object and some entity of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableFunction<C, O> getter,
																		   EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relation between current embeddable object and some entity of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																		   EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a relation between current embeddable object and some entity of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param getter the way to get the {@link Set} from the source embeddable object
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Collection<O>>
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	/**
	 * Declares a relation between current embeddable object and some entity of type {@code O} through a {@link Collection}.
	 * Depending on collection type, order persistence can be asked by one of the {@link OneToManyOptions#indexed()}
	 * methods.
	 * Note that given mapping configuration has a generic signature made of {@code ? super O} to handle polymorphic case: given persister is allowed
	 * to handle any super type of current entity type.
	 *
	 * @param setter the way to set the {@link Set} from the source embeddable object
	 * @param mappingConfiguration the mapping configuration of the {@link Set} entities
	 * @param <O> type of {@link Collection} element
	 * @param <J> type of identifier of {@code O}
	 * @param <S> refined {@link Collection} type
	 * @return an enhanced version of {@code this} so one can add set options to the relation or add mapping to {@code this}
	 * @see #mapOneToMany(SerializableFunction, EntityMappingConfigurationProvider)
	 */
	<O, J, S extends Collection<O>>
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	/**
	 * Declares a direct relation between current embeddable object and some entity of type {@code O}.
	 *
	 * @param getter the way to get the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableFunction<C, O> getter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a many-to-one relation between current embeddable object and some entity of type {@code O}.
	 *
	 * @param setter the way to set the target entity
	 * @param mappingConfiguration the mapping configuration of the target entity
	 * @param <O> type of target entity
	 * @param <J> type of identifier of {@code O}
	 * @return an enhanced version of {@code this} so one can add options to the relation or add mapping to {@code this}
	 */
	<O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableBiConsumer<C, O> setter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	/**
	 * Declares a many-to-many relation between current embeddable object and some entity of type {@code O}.
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
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2>
	mapManyToMany(SerializableFunction<C, S1> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	/**
	 * Declares a many-to-many relation between current embeddable object and some entity of type {@code O}.
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
	FluentEmbeddableMappingBuilderManyToManyOptions<C, O, S1, S2>
	mapManyToMany(SerializableBiConsumer<C, S1> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	/**
	 * Change default column naming strategy, which is {@link ColumnNamingStrategy#DEFAULT}, by the given one.
	 * <strong>Please note that this setting must be done at very first time before adding any mapping, else it won't be taken into account</strong>
	 * 
	 * @param columnNamingStrategy a new {@link ColumnNamingStrategy} (non null)
	 * @return this
	 */
	FluentEmbeddableMappingConfiguration<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	FluentEmbeddableMappingConfiguration<C> withIndexNaming(IndexNamingStrategy indexNamingStrategy);
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping"
	 * 
	 * @param <C> main bean type
	 * @param <O> embedded bean type
	 * @see #embed(SerializableFunction, EmbeddableMappingConfigurationProvider)  
	 * @see #embed(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)   
	 */
	interface FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O>
			extends FluentEmbeddableMappingConfiguration<C>, ImportedEmbedOptions<O> {
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableFunction<O, IN> getter, Size columnSize);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableBiConsumer<O, IN> setter, Size columnSize);
		
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(AccessorChain<O, IN> chain, Size columnSize);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
	interface FluentEmbeddableMappingConfigurationEnumOptions<C, E extends Enum<E>> extends FluentEmbeddableMappingConfiguration<C>, EnumOptions<E> {
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> byName();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> byOrdinal();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> mandatory();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> unique();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> readonly();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> columnName(String name);
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> columnSize(Size size);
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> column(Column<? extends Table, ? extends E> column);
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> fieldName(String name);
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> readConverter(Converter<E, E> converter);
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> writeConverter(Converter<E, E> converter);
		
		@Override
		<V> FluentEmbeddableMappingConfigurationEnumOptions<C, E> sqlBinder(ParameterBinder<V> parameterBinder);
	}
	
	interface FluentEmbeddableMappingConfigurationPropertyOptions<C, O> extends FluentEmbeddableMappingConfiguration<C>, PropertyOptions<O> {
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> mandatory();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> unique();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> setByConstructor();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> readonly();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> columnName(String name);
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> columnSize(Size size);
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> column(Column<? extends Table, ? extends O> column);
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> fieldName(String name);
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> readConverter(Converter<O, O> converter);
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> writeConverter(Converter<O, O> converter);
		
		@Override
		<V> FluentEmbeddableMappingConfigurationPropertyOptions<C, O> sqlBinder(ParameterBinder<V> parameterBinder);
	}
}
