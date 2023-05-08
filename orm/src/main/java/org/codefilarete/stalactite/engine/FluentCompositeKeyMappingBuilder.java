package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorChain;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link FluentCompositeKeyMappingConfiguration} and {@link CompositeKeyMappingConfigurationProvider}
 * 
 * @author Guillaume Mary
 */
public interface FluentCompositeKeyMappingBuilder<C> extends FluentCompositeKeyMappingConfiguration<C>, CompositeKeyMappingConfigurationProvider<C> {
	
	/* Overwriting methods signature to return a type that aggregates options of this class */
	
	<O> FluentCompositeKeyMappingBuilder<C> map(SerializableBiConsumer<C, O> setter);
	
	<O> FluentCompositeKeyMappingBuilder<C> map(SerializableFunction<C, O> getter);
	
	<O> FluentCompositeKeyMappingBuilder<C> map(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> FluentCompositeKeyMappingBuilder<C> map(SerializableFunction<C, O> getter, String columnName);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter, String columnName);
	
	FluentCompositeKeyMappingBuilder<C> mapSuperClass(CompositeKeyMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableFunction<C, O> getter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableBiConsumer<C, O> setter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	FluentCompositeKeyMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	interface FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>
			extends FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>,
			FluentCompositeKeyMappingBuilder<C> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
	}
	
	interface FluentCompositeKeyMappingBuilderEnumOptions<C>
			extends FluentCompositeKeyMappingConfigurationEnumOptions<C>, FluentCompositeKeyMappingBuilder<C> {
		
		@Override
		FluentCompositeKeyMappingBuilderEnumOptions<C> byName();
		
		@Override
		FluentCompositeKeyMappingBuilderEnumOptions<C> byOrdinal();
		
	}
}