package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract to define options when reusing a configuration of an embeddable, very close to {@link EmbedWithColumnOptions}
 * without {@link EmbedOptions#innerEmbed(SerializableFunction)} possibility (because one can't "inner mebed" something that was externally defined).
 * 
 * @author Guillaume Mary
 * @see IFluentEmbeddableMappingConfiguration#embed(SerializableBiConsumer, EmbeddedBeanMappingStrategyBuilder)
 */
public interface ImportedEmbedOptions<C> {
	
	/**
	 * Overrides embedding with a column name
	 *
	 * @param function the getter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName);
	
	<IN> ImportedEmbedOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName);
	
	<IN> ImportedEmbedOptions<C> exclude(SerializableBiConsumer<C, IN> setter);
	
	<IN> ImportedEmbedOptions<C> exclude(SerializableFunction<C, IN> getter);
	
}
