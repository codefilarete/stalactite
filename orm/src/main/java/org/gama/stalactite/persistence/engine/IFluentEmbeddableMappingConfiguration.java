package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorChain;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare the persistence mapping of a class as an embedded bean.
 *
 * @param <C> type that owns method  
 * 
 * @author Guillaume Mary
 * @see IFluentEmbeddableMappingBuilder
 */
public interface IFluentEmbeddableMappingConfiguration<C> {
	
	/**
	 * Adds a property to be mapped.
	 * By default column name will be extracted from setter according to the Java Bean convention naming.
	 * 
	 * @param setter a Method Reference to a setter
	 * @param <O> setter return type / property type to be mapped
	 * @return this
	 * @see #columnNamingStrategy(ColumnNamingStrategy) 
	 */
	<O> IFluentEmbeddableMappingConfigurationPropertyOptions<C> add(SerializableBiConsumer<C, O> setter);
	
	/**
	 * Adds a property to be mapped. Column name will be extracted from getter according to the Java Bean convention naming.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 * @see #columnNamingStrategy(ColumnNamingStrategy)
	 */
	<O> IFluentEmbeddableMappingConfigurationPropertyOptions<C> add(SerializableFunction<C, O> getter);
	
	/**
	 * Adds a property to be mapped and overrides its default column name.
	 *
	 * @param setter a Method Reference to a setter
	 * @param <O> setter return type / property type to be mapped
	 * @return this
	 */
	<O> IFluentEmbeddableMappingConfigurationPropertyOptions<C> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	/**
	 * Adds a property to be mapped and overrides its default column name.
	 *
	 * @param getter a Method Reference to a getter
	 * @param <O> getter input type / property type to be mapped
	 * @return this
	 */
	<O> IFluentEmbeddableMappingConfigurationPropertyOptions<C> add(SerializableFunction<C, O> getter, String columnName);
	
	<E extends Enum<E>> IFluentEmbeddableMappingConfigurationEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter);
	
	<E extends Enum<E>> IFluentEmbeddableMappingConfigurationEnumOptions<C> addEnum(SerializableFunction<C, E> getter);
	
	<E extends Enum<E>> IFluentEmbeddableMappingConfigurationEnumOptions<C> addEnum(SerializableBiConsumer<C, E> setter, String columnName);
	
	<E extends Enum<E>> IFluentEmbeddableMappingConfigurationEnumOptions<C> addEnum(SerializableFunction<C, E> getter, String columnName);
	
	/**
	 * Please note that we can't create a generic type for {@code ? super C} by prefixing the method signature with {@code <X super C>}
	 * because it is not syntaxically valid (in Java 8). So it can't be mutualized between the 2 arguments {@code superType} and
	 * {@code mappingStrategy}. So user must be carefful to ensure by himself that both types are equal.
	 */
	IFluentEmbeddableMappingConfiguration<C> mapSuperClass(EmbeddableMappingConfiguration<? super C> superMappingConfiguration);
	
	<O> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter);

	<O> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> embed(SerializableFunction<C, O> getter);
	
	<O> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter,
																			  EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	<O> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter,
																			  EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	/**
	 * Change default column naming strategy, which is {@link ColumnNamingStrategy#DEFAULT}, by the given one.
	 * <strong>Please note that this setting must be done at very first time before adding any mapping, else it won't be taken into account</strong>
	 * 
	 * @param columnNamingStrategy a new {@link ColumnNamingStrategy} (non null)
	 * @return this
	 */
	IFluentEmbeddableMappingConfiguration<C> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an embedded bean.
	 * 
	 * @param <C> main bean type
	 * @param <O> embedded bean type
	 */
	interface IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> extends IFluentEmbeddableMappingConfiguration<C>, EmbedOptions<O> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param getter the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param getter the complex-typed property getter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Setter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param setter the complex-typed property setter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, IN> innerEmbed(SerializableBiConsumer<O, IN> setter);
		
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
	}
	
	/**
	 * A mashup that allows to come back to the "main" options as well as continue configuration of an "imported bean mapping"
	 * 
	 * @param <C> main bean type
	 * @param <O> embedded bean type
	 * @see #embed(SerializableFunction, EmbeddedBeanMappingStrategyBuilder)  
	 * @see #embed(SerializableBiConsumer, EmbeddedBeanMappingStrategyBuilder)   
	 */
	interface IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O>
			extends IFluentEmbeddableMappingConfiguration<C>, ImportedEmbedOptions<O> {
		
		@Override
		<IN> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> function, String columnName);
		
		<IN> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> overrideName(AccessorChain<O, IN> chain, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
		
		@Override
		<IN> IFluentEmbeddableMappingConfigurationImportedEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
	}
	
	interface IFluentEmbeddableMappingConfigurationEnumOptions<C> extends IFluentEmbeddableMappingConfiguration<C>, EnumOptions {
		
		@Override
		IFluentEmbeddableMappingConfigurationEnumOptions<C> byName();
		
		@Override
		IFluentEmbeddableMappingConfigurationEnumOptions<C> byOrdinal();
		
		@Override
		IFluentEmbeddableMappingConfigurationEnumOptions<C> mandatory();
	}
	
	interface IFluentEmbeddableMappingConfigurationPropertyOptions<C> extends IFluentEmbeddableMappingConfiguration<C>, PropertyOptions {
		
		@Override
		IFluentEmbeddableMappingConfigurationPropertyOptions mandatory();
	}
}
