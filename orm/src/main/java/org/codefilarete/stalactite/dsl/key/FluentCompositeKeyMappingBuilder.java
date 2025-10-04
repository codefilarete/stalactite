package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;
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
	
	<O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializableBiConsumer<C, O> setter);
	
	<O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter);
	
	FluentCompositeKeyMappingBuilder<C> mapSuperClass(CompositeKeyMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableFunction<C, O> getter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableBiConsumer<C, O> setter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	FluentCompositeKeyMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	interface FluentCompositeKeyMappingBuilderPropertyOptions<C> extends FluentCompositeKeyMappingBuilder<C>,
			FluentCompositeKeyMappingConfigurationPropertyOptions<C> {
		
		@Override
		FluentCompositeKeyMappingBuilderPropertyOptions<C> columnName(String name);
		
		@Override
		FluentCompositeKeyMappingBuilderPropertyOptions<C> fieldName(String name);
	}
	
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
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableFunction<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableBiConsumer<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(AccessorChain<O, IN> chain, Size columnSize);
		
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
		
		@Override
		FluentCompositeKeyMappingBuilderEnumOptions<C> columnName(String name);
		
		@Override
		FluentCompositeKeyMappingBuilderEnumOptions<C> fieldName(String name);
	}
}