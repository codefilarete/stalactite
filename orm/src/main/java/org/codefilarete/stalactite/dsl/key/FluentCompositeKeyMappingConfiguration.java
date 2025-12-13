package org.codefilarete.stalactite.dsl.key;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.embeddable.ImportedEmbedOptions;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class as an embedded bean.
 *
 * @param <C> type that owns method  
 * 
 * @author Guillaume Mary
 * @see FluentCompositeKeyMappingBuilder
 */
public interface FluentCompositeKeyMappingConfiguration<C> {
	
	/**
	 * Adds a property to be mapped.
	 * By default column name will be extracted from setter according to the Java Bean convention naming.
	 * 
	 * @param setter a Method Reference to a setter
	 * @param <O> setter return type / property type to be mapped
	 * @return this
	 * @see #withColumnNaming(ColumnNamingStrategy)
	 */
	<O> FluentCompositeKeyMappingConfigurationPropertyOptions<C> map(SerializableBiConsumer<C, O> setter);
	
	/**
	 * Adds a property to be mapped. Column name will be extracted from getter according to the Java Bean convention naming.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 * @see #withColumnNaming(ColumnNamingStrategy)
	 */
	<O> FluentCompositeKeyMappingConfigurationPropertyOptions<C> map(SerializableFunction<C, O> getter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingConfigurationEnumOptions<C> mapEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> FluentCompositeKeyMappingConfigurationEnumOptions<C> mapEnum(SerializableFunction<C, E> getter);
	
	/**
	 * Please note that we can't create a generic type for {@code ? super C} by prefixing the method signature with {@code <X super C>}
	 * because it is not syntactically valid (in Java 8). So it can't be mutualized between the 2 arguments {@code superType} and
	 * {@code mappingStrategy}. So user must be careful to ensure by himself that both types are equal.
	 */
	FluentCompositeKeyMappingConfiguration<C> mapSuperClass(CompositeKeyMappingConfigurationProvider<? super C> superMappingConfiguration);
	
	<O> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableFunction<C, O> getter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	<O> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> embed(
			SerializableBiConsumer<C, O> setter,
			CompositeKeyMappingConfigurationProvider<? extends O> compositeKeyMappingBuilder);
	
	/**
	 * Change default column naming strategy, which is {@link ColumnNamingStrategy#DEFAULT}, by the given one.
	 * <strong>Please note that this setting must be done at very first time before adding any mapping, else it won't be taken into account</strong>
	 * 
	 * @param columnNamingStrategy a new {@link ColumnNamingStrategy} (non null)
	 * @return this
	 */
	FluentCompositeKeyMappingConfiguration<C> withColumnNaming(ColumnNamingStrategy columnNamingStrategy);
	
	interface FluentCompositeKeyMappingConfigurationPropertyOptions<C> extends FluentCompositeKeyMappingConfiguration<C>, CompositeKeyPropertyOptions {
		
		@Override
		FluentCompositeKeyMappingConfigurationPropertyOptions<C> columnName(String name);
		
		@Override
		FluentCompositeKeyMappingConfigurationPropertyOptions<C> columnSize(Size size);
		
		@Override
		FluentCompositeKeyMappingConfigurationPropertyOptions<C> fieldName(String name);
	}
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping"
	 * 
	 * @param <C> main bean type
	 * @param <O> embedded bean type
	 * @see #embed(SerializableFunction, CompositeKeyMappingConfigurationProvider)  
	 * @see #embed(SerializableBiConsumer, CompositeKeyMappingConfigurationProvider)   
	 */
	interface FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O>
			extends FluentCompositeKeyMappingConfiguration<C>, ImportedEmbedOptions<O> {
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableFunction<O, IN> getter, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(SerializableBiConsumer<O, IN> setter, Size columnSize);
		
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> overrideSize(AccessorChain<O, IN> chain, Size columnSize);
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
		@Override
		<IN> FluentCompositeKeyMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
	interface FluentCompositeKeyMappingConfigurationEnumOptions<C> extends FluentCompositeKeyMappingConfiguration<C>, CompositeKeyEnumOptions, CompositeKeyPropertyOptions {
		
		@Override
		FluentCompositeKeyMappingConfigurationEnumOptions<C> byName();
		
		@Override
		FluentCompositeKeyMappingConfigurationEnumOptions<C> byOrdinal();
		
		@Override
		FluentCompositeKeyMappingConfigurationEnumOptions<C> columnName(String name);
		
		@Override
		FluentCompositeKeyMappingConfigurationEnumOptions<C> fieldName(String name);
	}
	
	interface CompositeKeyEnumOptions {
		
		CompositeKeyEnumOptions byName();
		
		CompositeKeyEnumOptions byOrdinal();
	}
}