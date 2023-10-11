package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
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
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableBiConsumer<C, O> setter);
	
	<O> FluentEmbeddableMappingBuilderPropertyOptions<C> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentEmbeddableMappingBuilderEnumOptions<C> mapEnum(SerializableFunction<C, E> getter);
	
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
		
		@Override
		FluentEmbeddableMappingBuilderEnumOptions<C> readonly();
		
	}
	
	interface FluentEmbeddableMappingBuilderPropertyOptions<C> extends FluentEmbeddableMappingConfigurationPropertyOptions<C>, FluentEmbeddableMappingBuilder<C> {
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> mandatory();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> setByConstructor();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> readonly();
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> columnName(String name);
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> column(Column<? extends Table, ?> column);
		
		@Override
		FluentEmbeddableMappingBuilderPropertyOptions<C> fieldName(String name);
	}
}
