package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorChain;
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
	
	interface FluentEmbeddableMappingConfigurationEnumOptions<C, E extends Enum<E>> extends FluentEmbeddableMappingConfiguration<C>, EnumOptions<E> {
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> byName();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> byOrdinal();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> mandatory();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> readonly();
		
		@Override
		FluentEmbeddableMappingConfigurationEnumOptions<C, E> columnName(String name);
		
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
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> setByConstructor();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> readonly();
		
		@Override
		FluentEmbeddableMappingConfigurationPropertyOptions<C, O> columnName(String name);
		
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
