package org.codefilarete.stalactite.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.reflection.AccessorChain;

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
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C> map(SerializableBiConsumer<C, O> setter);
	
	/**
	 * Adds a property to be mapped. Column name will be extracted from getter according to the Java Bean convention naming.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 * @see #withColumnNaming(ColumnNamingStrategy)
	 */
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C> map(SerializableFunction<C, O> getter);
	
	/**
	 * Adds a property to be mapped and overrides its default column name.
	 *
	 * @param setter a Method Reference to a setter
	 * @param <O> setter return type / property type to be mapped
	 * @return this
	 */
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C> map(SerializableBiConsumer<C, O> setter, String columnName);
	
	/**
	 * Adds a property to be mapped and overrides its default column name.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 */
	<O> FluentEmbeddableMappingConfigurationPropertyOptions<C> map(SerializableFunction<C, O> getter, String columnName);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C> mapEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> FluentEmbeddableMappingConfigurationEnumOptions<C> mapEnum(SerializableFunction<C, E> getter, String columnName);
	
	/**
	 * Please note that we can't create a generic type for {@code ? super C} by prefixing the method signature with {@code <X super C>}
	 * because it is not syntaxically valid (in Java 8). So it can't be mutualized between the 2 arguments {@code superType} and
	 * {@code mappingStrategy}. So user must be carefful to ensure by himself that both types are equal.
	 */
	FluentEmbeddableMappingConfiguration<C> mapSuperClass(EmbeddableMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	<O> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																			 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	<O> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																			 EmbeddableMappingConfigurationProvider<? extends O> embeddableMappingBuilder);
	
	/**
	 * Change default column naming strategy, which is {@link ColumnNamingStrategy#DEFAULT}, by the given one.
	 * <strong>Please note that this setting must be done at very first time before adding any mapping, else it won't be taken into account</strong>
	 * 
	 * @param columnNamingStrategy a new {@link ColumnNamingStrategy} (non null)
	 * @return this
	 */
	FluentEmbeddableMappingConfiguration<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
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
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> FluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
	interface FluentEmbeddableMappingConfigurationEnumOptions<C> extends FluentEmbeddableMappingConfiguration<C>, EnumOptions {
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C> byName();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C> byOrdinal();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C> mandatory();
	}
	
	interface FluentEmbeddableMappingConfigurationPropertyOptions<C> extends FluentEmbeddableMappingConfiguration<C>, PropertyOptions {
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C> mandatory();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C> setByConstructor();
	}
}
