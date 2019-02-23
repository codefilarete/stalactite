package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface IFluentEmbeddableMappingBuilder<C> extends IFluentEmbeddableMappingConfiguration<C>, EmbeddedBeanMappingStrategyBuilder<C> {
	
	/* Overwritten methods for return type that must match this class */
	
	<O> IFluentEmbeddableMappingBuilder<C> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, O> getter);
	
	<O> IFluentEmbeddableMappingBuilder<C> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, O> function, String columnName);
	
	IFluentEmbeddableMappingBuilder<C> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy);
	
	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableBiConsumer<C, O> setter);

	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C, O> embed(SerializableFunction<C, O> getter);
	
	/**
	 * Crossover between {@link IFluentEmbeddableMappingConfigurationEmbedOptions} (refine its return types) and {@link IFluentEmbeddableMappingBuilder}
	 * in order that {@link #embed(SerializableFunction)}
	 * methods result can chain with some {@link EmbedOptions} as well as continue configuratio of an {@link IFluentEmbeddableMappingConfiguration}
	 * 
	 * @param <T> owner type
	 * @param <O> type of the property that must be overriden
	 */
	interface IFluentEmbeddableMappingBuilderEmbedOptions<T, O>
			extends IFluentEmbeddableMappingConfigurationEmbedOptions<T, O>,
					EmbeddedBeanMappingStrategyBuilder<T>,	// This is superfluous because IFluentEmbeddableMappingBuilder already extends it.
					IFluentEmbeddableMappingBuilder<T> {	// This is necessary to benefit from refined return types, else API is broken (see dedicated unit tests).
		
		/**
		 * Overrides embedding with a column name
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 * 
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 * 
		 * @param getter the complex-typed property getter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Setter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param setter the complex-typed property setter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T, IN> innerEmbed(SerializableBiConsumer<O, IN> setter);
		
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T, O> exclude(SerializableFunction<O, IN> getter);
		
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T, O> exclude(SerializableBiConsumer<O, IN> setter);
	}
	
}
