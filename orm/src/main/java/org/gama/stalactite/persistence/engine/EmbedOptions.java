package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for configuring embedded object
 * 
 * @author Guillaume Mary
 * @see IFluentEmbeddableMappingConfiguration#embed(SerializableFunction) 
 */
public interface EmbedOptions<C> {
	
	/**
	 * Overrides embedding with a column name
	 *
	 * @param getter the getter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> EmbedOptions<C> overrideName(SerializableFunction<C, IN> getter, String columnName);
	
	<IN> EmbedOptions<C> overrideName(SerializableBiConsumer<C, IN> setter, String columnName);
	
	/**
	 * Adds a complex-typed property as embedded into this embedded
	 * Getter must have a matching field (Java bean naming convention) in its declaring class.
	 *
	 * @param getter the complex-typed property getter as a method reference
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> EmbedOptions<IN> innerEmbed(SerializableFunction<C, IN> getter);
	
	/**
	 * Adds a complex-typed property as embedded into this embedded
	 * Setter must have a matching field (Java bean naming convention) in its declaring class.
	 *
	 * @param setter the complex-typed property setter as a method reference
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> EmbedOptions<IN> innerEmbed(SerializableBiConsumer<C, IN> setter);
	
	<IN> EmbedOptions<C> exclude(SerializableFunction<C, IN> getter);
	
	<IN> EmbedOptions<C> exclude(SerializableBiConsumer<C, IN> setter);
}