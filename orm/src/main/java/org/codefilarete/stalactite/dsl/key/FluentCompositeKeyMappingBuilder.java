package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.Size;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link FluentCompositeKeyMappingConfiguration} and {@link CompositeKeyMappingConfigurationProvider}
 * 
 * @author Guillaume Mary
 */
public interface FluentCompositeKeyMappingBuilder<C> extends FluentCompositeKeyMappingConfiguration<C>, CompositeKeyMappingConfigurationProvider<C> {
	
	/* Overwriting methods signature to return a type that aggregates options of this class */
	
	@Override
	<O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializablePropertyMutator<C, O> setter);
	
	@Override
	<O> FluentCompositeKeyMappingBuilderPropertyOptions<C> map(SerializablePropertyAccessor<C, O> getter);
	
	@Override
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializablePropertyMutator<C, E> setter);
	
	@Override
	<E extends Enum<E>> FluentCompositeKeyMappingBuilderEnumOptions<C> mapEnum(SerializablePropertyAccessor<C, E> getter);
	
	@Override
	FluentCompositeKeyMappingBuilder<C> mapSuperClass(CompositeKeyMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	@Override
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializablePropertyAccessor<C, O> getter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	@Override
	<O> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializablePropertyMutator<C, O> setter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	@Override
	FluentCompositeKeyMappingBuilder<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	interface FluentCompositeKeyMappingBuilderPropertyOptions<C> extends FluentCompositeKeyMappingBuilder<C>,
			FluentCompositeKeyMappingConfigurationPropertyOptions<C> {
		
		@Override
		FluentCompositeKeyMappingBuilderPropertyOptions<C> columnName(String name);
		
		@Override
		FluentCompositeKeyMappingBuilderPropertyOptions<C> columnSize(Size size);
		
		@Override
		FluentCompositeKeyMappingBuilderPropertyOptions<C> fieldName(String name);
	}
	
	interface FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>
			extends FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>,
			FluentCompositeKeyMappingBuilder<C> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializablePropertyAccessor<O, IN> function, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializablePropertyMutator<O, IN> function, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializablePropertyAccessor<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializablePropertyMutator<O, IN> function, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(AccessorChain<O, IN> chain, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializablePropertyMutator<O, IN> setter);
		
		@Override
		<IN> FluentCompositeKeyMappingBuilderCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializablePropertyAccessor<O, IN> getter);
		
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
