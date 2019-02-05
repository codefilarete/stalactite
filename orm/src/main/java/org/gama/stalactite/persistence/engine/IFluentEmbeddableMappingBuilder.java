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
	
	IFluentEmbeddableMappingBuilder<C> add(SerializableFunction<C, ?> function, String columnName);
	
	IFluentEmbeddableMappingBuilder<C> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy);
	
	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C> embed(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentEmbeddableMappingBuilderEmbedOptions<C> embed(SerializableFunction<C, O> getter);
	
	interface IFluentEmbeddableMappingBuilderEmbedOptions<T> extends IFluentEmbeddableMappingConfigurationEmbedOptions<T>, EmbeddedBeanMappingStrategyBuilder<T> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingBuilderEmbedOptions<T> overrideName(SerializableFunction<IN, ?> function, String columnName);
	}
	
}
