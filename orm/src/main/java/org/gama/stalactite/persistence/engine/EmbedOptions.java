package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for configuring embedded object
 * 
 * @author Guillaume Mary
 */
public interface EmbedOptions<T extends Identified, I extends StatefullIdentifier> {
	
	/**
	 * Overrides embedding with an existing column
	 *
	 * @param function the getter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> IFluentMappingBuilderEmbedOptions<T, I> overrideName(SerializableFunction<IN, ?> function, String columnName);
	
	/**
	 * Overrides embedding with an existing target column
	 * 
	 * @param function the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @param <OUT> ouput of the function (property type)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN, OUT> IFluentMappingBuilderEmbedOptions<T, I> override(SerializableFunction<IN, OUT> function, Column<Table, OUT> targetColumn);
}