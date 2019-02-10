package org.gama.stalactite.persistence.engine;

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
	 * @param <OUT> ouput of the function (property type)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN, OUT> EmbedWithColumnOptions<T> override(SerializableFunction<T, IN> function, Column<Table, OUT> targetColumn);
}