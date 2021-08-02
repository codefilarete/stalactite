package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorChain;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link FluentEmbeddableMappingConfiguration} and {@link EmbeddableMappingConfigurationProvider}
 * 
 * @author Guillaume Mary
 */
public interface FluentEmbeddableMappingBuilder<C> extends FluentEmbeddableMappingConfiguration<C>, EmbeddableMappingConfigurationProvider<C> {
	
	/* Overwritting methods signature to return a type that aggregates options of this class */
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter);
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter);
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter, String columnName);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	FluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	<O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																									 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	FluentEmbeddableMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
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
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> FluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
	}
	
	interface FluentEmbeddableMappingBuilderEnumOptions<C>
			extends FluentEmbeddableMappingConfigurationEnumOptions<C>,
		FluentEmbeddableMappingBuilder<C> {
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C> byName();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C> byOrdinal();
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C> mandatory();
		
	}
	
	interface FluentEmbeddableMappingBuilderPropertyOptions<C> extends FluentEmbeddableMappingConfigurationPropertyOptions<C>, FluentEmbeddableMappingBuilder<C> {
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> mandatory();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C> setByConstructor();
	}
}
