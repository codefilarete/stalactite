package org.codefilarete.stalactite.dsl.embeddable;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for configuring embedded object
 * 
 * @author Guillaume Mary
 * @see FluentEmbeddableMappingConfiguration#embed(SerializableFunction, EmbeddableMappingConfigurationProvider)
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
	
	<IN> EmbedOptions<C> exclude(SerializableFunction<C, IN> getter);
	
	<IN> EmbedOptions<C> exclude(SerializableBiConsumer<C, IN> setter);
}