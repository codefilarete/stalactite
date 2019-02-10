package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface IFluentEmbeddableMappingConfiguration<C> {
	
	<O> IFluentEmbeddableMappingConfiguration<C> add(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentEmbeddableMappingConfiguration<C> add(SerializableFunction<C, O> getter);
	
	<O> IFluentEmbeddableMappingConfiguration<C> add(SerializableBiConsumer<C, O> setter, String columnName);
	
	<O> IFluentEmbeddableMappingConfiguration<C> add(SerializableFunction<C, O> function, String columnName);
	
	/**
	 * Please note that we can't create a generic type for {@code ? super C} by prefixing the method signature with {@code <X super C>}
	 * because it is not syntaxically valid (in Java 8). So it can't be mutualized between the 2 arguments {@code superType} and
	 * {@code mappingStrategy}. So user must be carefful to ensure by himself that both types are equal.
	 */
	IFluentEmbeddableMappingConfiguration<C> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy);
	
	<O> IFluentEmbeddableMappingConfiguration<C> embed(SerializableBiConsumer<C, O> setter);

	<O> IFluentEmbeddableMappingConfiguration<C> embed(SerializableFunction<C, O> getter);
	
	/**
	 * Crossover between {@link IFluentEmbeddableMappingConfiguration} and {@link EmbedOptions} in order that {@link #embed(SerializableFunction)} methods
	 * result can chain with some {@link EmbedOptions} as well as continue configuratio of an {@link IFluentEmbeddableMappingConfiguration}
	 * 
	 * @param <T> owner type
	 * @param <O> type of the property that must be overriden
	 */
	interface IFluentEmbeddableMappingConfigurationEmbedOptions<T, O> extends IFluentEmbeddableMappingConfiguration<T>, EmbedOptions<O> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<T, O> overrideName(SerializableFunction<O, IN> function, String columnName);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Getter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param getter the complex-typed property getter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<T, IN> innerEmbed(SerializableFunction<O, IN> getter);
		
		/**
		 * Adds a complex-typed property as embedded into this embedded
		 * Setter must have a matching field (Java bean naming convention) in its declaring class.
		 *
		 * @param setter the complex-typed property setter as a method reference
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<T, IN> innerEmbed(SerializableBiConsumer<O, IN> setter);
		
	}
}
