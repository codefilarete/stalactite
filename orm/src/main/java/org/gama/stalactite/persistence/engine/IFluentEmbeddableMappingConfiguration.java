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
	
	IFluentEmbeddableMappingConfiguration<C> add(SerializableFunction<C, ?> function, String columnName);
	
	IFluentEmbeddableMappingConfiguration<C> mapSuperClass(Class<? super C> superType, EmbeddedBeanMappingStrategy<? super C, ?> mappingStrategy);
	
	<O> IFluentEmbeddableMappingConfigurationEmbedOptions<C> embed(SerializableBiConsumer<C, O> setter);
	
	<O> IFluentEmbeddableMappingConfigurationEmbedOptions<C> embed(SerializableFunction<C, O> getter);
	
	interface IFluentEmbeddableMappingConfigurationEmbedOptions<T> extends IFluentEmbeddableMappingConfiguration<T>, EmbedOptions<T> {
		
		/**
		 * Overrides embedding with an existing column
		 *
		 * @param function the getter as a method reference
		 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
		 * @param <IN> input of the function (type of the embedded element)
		 * @return a mapping configurer, specialized for embedded elements
		 */
		@Override
		<IN> IFluentEmbeddableMappingConfigurationEmbedOptions<T> overrideName(SerializableFunction<IN, ?> function, String columnName);
	}
}
