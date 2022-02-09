package org.codefilarete.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * Equivalent of {@link ImportedEmbedOptions} in context of an entity : allow to define some {@link Column} for some properties.
 * Those methods are not available in {@link ImportedEmbedOptions} because it is supposed to be reusable in different entities and therefore for
 * different tables, hence letting one define column for an embeddable bean makes no sense.
 * 
 * @author Guillaume Mary
 */
public interface ImportedEmbedWithColumnOptions<C> extends ImportedEmbedOptions<C> {
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param getter the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializableFunction<C, IN> getter, Column<? extends Table, IN> targetColumn);
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param setter the setter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializableBiConsumer<C, IN> setter, Column<? extends Table, IN> targetColumn);
	
}
