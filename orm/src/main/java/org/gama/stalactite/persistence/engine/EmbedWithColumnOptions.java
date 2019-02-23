package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for configuring embedded object
 * 
 * @author Guillaume Mary
 */
public interface EmbedWithColumnOptions<T> extends EmbedOptions<T> {
	
	/**
	 * Overrides embedding with an existing target column
	 * 
	 * @param function the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> EmbedWithColumnOptions<T> override(SerializableFunction<T, IN> function, Column<Table, IN> targetColumn);
	
	<IN> EmbedWithColumnOptions<T> exclude(SerializableFunction<T, IN> getter);
	
	<IN> EmbedWithColumnOptions<T> exclude(SerializableBiConsumer<T, IN> setter);
	
}