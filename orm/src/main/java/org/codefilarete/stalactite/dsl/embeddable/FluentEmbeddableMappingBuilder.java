package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link FluentEmbeddableMappingConfiguration} and {@link EmbeddableMappingConfigurationProvider}
 * 
 * @author Guillaume Mary
 */
public interface FluentEmbeddableMappingBuilder<C> extends FluentEmbeddableMappingConfiguration<C>, EmbeddableMappingConfigurationProvider<C> {
	
	/* Overwriting methods signature to return a type that aggregates options of this class */
	
	@Override
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C, O> map(SerializableBiConsumer<C, O> setter);
	
	@Override
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C, O> map(SerializableFunction<C, O> getter);
	
	@Override
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> mapEnum(SerializableBiConsumer<C, E> setter);
	
	@Override
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C, E> mapEnum(SerializableFunction<C, E> getter);
	
	@Override
	FluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	@Override
	<O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	@Override
	<O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	@Override
	<O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableFunction<C, O> getter,
																		   EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	@Override
	<O, J> FluentEmbeddableMappingBuilderOneToOneOptions<C, O> mapOneToOne(SerializableBiConsumer<C, O> setter,
																		   EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	@Override
	<O, J, S extends Collection<O>>
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S>
	mapOneToMany(SerializableFunction<C, S> getter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	@Override
	<O, J, S extends Collection<O>>
	FluentEmbeddableMappingBuilderOneToManyOptions<C, O, S>
	mapOneToMany(SerializableBiConsumer<C, S> setter, EntityMappingConfigurationProvider<? super O, J> mappingConfiguration);
	
	@Override
	<O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableFunction<C, O> getter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	@Override
	<O, J, S extends Collection<C>>
	FluentEmbeddableMappingBuilderManyToOneOptions<C, O, S>
	mapManyToOne(SerializableBiConsumer<C, O> setter,
				 EntityMappingConfigurationProvider<? extends O, J> mappingConfiguration);
	
	@Override
	FluentEmbeddableMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	@Override
	FluentEmbeddableMappingBuilder<C> withUniqueConstraintNaming(UniqueConstraintNamingStrategy uniqueConstraintNamingStrategy);
	
	interface FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O>
			extends FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O>,
		FluentEmbeddableMappingBuilder<C> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableFunction<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableBiConsumer<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideSize(AccessorChain<O, IN> chain, Size columnSize);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
	}
	
	interface FluentEmbeddableMappingBuilderEnumOptions<C, E extends Enum<E>>
			extends FluentEmbeddableMappingConfigurationEnumOptions<C, E>, FluentEmbeddableMappingBuilder<C> {
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> byName();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> byOrdinal();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> mandatory();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> unique();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> readonly();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> columnName(String name);
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> columnSize(Size size);
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> column(Column<? extends Table, ? extends E> column);
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C, E> fieldName(String name);
		
		@Override
		<X> FluentEmbeddableMappingBuilderEnumOptions<C, E> readConverter(Converter<X, E> converter);
		
		@Override
		<X> FluentEmbeddableMappingBuilderEnumOptions<C, E> writeConverter(Converter<E, X> converter);
		
		@Override
		<V> FluentEmbeddableMappingBuilderEnumOptions<C, E> sqlBinder(ParameterBinder<V> parameterBinder);
	}
	
	interface FluentEmbeddableMappingBuilderPropertyOptions<C, O> extends FluentEmbeddableMappingConfigurationPropertyOptions<C, O>, FluentEmbeddableMappingBuilder<C> {
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> mandatory();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> nullable();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> unique();

		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> setByConstructor();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> readonly();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> columnName(String name);
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> columnSize(Size size);
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> column(Column<? extends Table, ? extends O> column);
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C, O> fieldName(String name);
		
		@Override
		<X> FluentEmbeddableMappingBuilderPropertyOptions<C, O> readConverter(Converter<X, O> converter);
		
		@Override
		<X> FluentEmbeddableMappingBuilderPropertyOptions<C, O> writeConverter(Converter<O, X> converter);
		
		@Override
		<V> FluentEmbeddableMappingBuilderPropertyOptions<C, O> sqlBinder(ParameterBinder<V> parameterBinder);
	}
}
