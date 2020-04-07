package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ImportedEmbedWithColumnOptions<C> extends ImportedEmbedOptions<C> {
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param function the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializableFunction<C, IN> function, Column<Table, IN> targetColumn);
}
