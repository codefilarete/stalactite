package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorChain;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link IFluentEmbeddableMappingConfiguration} and {@link EmbeddableMappingConfigurationProvider}
 * 
 * @author Guillaume Mary
 */
public interface IFluentEmbeddableMappingBuilder<C> extends IFluentEmbeddableMappingConfiguration<C>, EmbeddableMappingConfigurationProvider<C> {
	
	/* Overwritting methods signature to return a type that aggregates options of this class */
	
	<O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter);
	
	<O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> IFluentEmbeddableMappingBuilderPropertyOptions<C> add(SerializableFunction<C, O> getter, String columnName);
	
	<E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> IFluentEmbeddableMappingBuilderEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	IFluentEmbeddableMappingBuilder<C> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration);
	
	<O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																									  EmbeddableMappingConfigurationProvider<O> embeddableMappingBuilder);
	
	<O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																									  EmbeddableMappingConfigurationProvider<O> embeddableMappingBuilder);
	
	IFluentEmbeddableMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	interface IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O>
			extends IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O>,
					IFluentEmbeddableMappingBuilder<C> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
	}
	
	interface IFluentEmbeddableMappingBuilderEnumOptions<C>
			extends IFluentEmbeddableMappingConfigurationEnumOptions<C>,
			IFluentEmbeddableMappingBuilder<C> {
		
		@Override
		IFluentEmbeddableMappingBuilderEnumOptions<C> byName();
		
		@Override
		IFluentEmbeddableMappingBuilderEnumOptions<C> byOrdinal();
		
		@Override
		IFluentEmbeddableMappingBuilderEnumOptions<C> mandatory();
		
	}
	
	interface IFluentEmbeddableMappingBuilderPropertyOptions<C> extends IFluentEmbeddableMappingConfigurationPropertyOptions<C>, IFluentEmbeddableMappingBuilder<C> {
		
		@Override
		IFluentEmbeddableMappingBuilderPropertyOptions<C> mandatory();
		
		@Override
		IFluentEmbeddableMappingConfigurationPropertyOptions<C> setByConstructor();
	}
}
