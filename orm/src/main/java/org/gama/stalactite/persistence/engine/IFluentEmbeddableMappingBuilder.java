package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.reflection.AccessorChain;
import org.gama.stalactite.persistence.structure.Table;

/**
 * An interface describing a fluent way to declare and build the persistence mapping of a class as an embedded bean.
 * This class mainly consists in mashup types between {@link IFluentEmbeddableMappingConfiguration} and {@link EmbeddedBeanMappingStrategyBuilder}
 * 
 * @author Guillaume Mary
 */
public interface IFluentEmbeddableMappingBuilder<C> extends IFluentEmbeddableMappingConfiguration<C>, EmbeddedBeanMappingStrategyBuilder<C> {
	
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
	
	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter);

	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableFunction<C, O> getter);
	
	<O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableFunction<C, O> getter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	<O> IFluentEmbeddableMappingBuilderEmbeddableMappingConfigurationImportedEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter, EmbeddedBeanMappingStrategyBuilder<O> embeddableMappingBuilder);
	
	IFluentEmbeddableMappingBuilder<C> columnNamingStrategy(ColumnNamingStrategy columnNamingStrategy);
	
	/**
	 * Crossover between {@link IFluentEmbeddableMappingConfigurationEmbedOptions} (refines its return types) and {@link IFluentEmbeddableMappingBuilder}
	 * in order that {@link #embed(SerializableFunction)} methods result can chain with some {@link EmbedOptions} as well as continue configuration
	 * of an {@link IFluentEmbeddableMappingConfiguration}
	 * 
	 * @param <C> owner type
	 * @param <O> type of the property that must be overriden
	 */
	interface IFluentEmbeddableMappingBuilderEmbedOptions<C, O>
			extends IFluentEmbeddableMappingConfigurationEmbedOptions<C, O>,
					IFluentEmbeddableMappingBuilder<C> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		/**
		 * Overrides embedding with a column name
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 * 
		 * @param getter the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> overrideName(SerializableFunction<O, IN> getter, String columnName);
		
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> overrideName(SerializableBiConsumer<O, IN> setter, String columnName);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 * 
		 * @param getter the complex-typed property getter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Setter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param setter the complex-typed property setter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, IN> innerEmbed(SerializableBiConsumer<O, IN> setter);
		
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> exclude(SerializableFunction<O, IN> getter);
		
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
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
	}
}
