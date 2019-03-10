package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface EmbedingEmbeddableOptions<C> {
	
	/**
	 * Overrides embedding with a column name
	 *
	 * @param function the getter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> EmbedingEmbeddableOptions<C> overrideName(SerializableFunction<C, IN> function, String columnName);
	
	<IN> EmbedingEmbeddableOptions<C> overrideName(SerializableBiConsumer<C, IN> function, String columnName);
	
}
