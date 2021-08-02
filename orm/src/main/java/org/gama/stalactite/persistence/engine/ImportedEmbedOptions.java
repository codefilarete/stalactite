package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract to define options when reusing a configuration of an embeddable, very close to {@link EmbedWithColumnOptions}
 * 
 * @author Guillaume Mary
 * @see FluentEmbeddableMappingConfiguration#embed(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)
 */
public interface ImportedEmbedOptions<C> {
	
	/**
	 * Overrides embedding with a column name
	 *
	 * @param getter the getter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedOptions<C> overrideName(SerializableFunction<C, IN> getter, String columnName);
	
	/**
	 * Overrides embedding with a column name
	 *
	 * @param setter the setter as a method reference
	 * @param columnName a column name that's the target of the getter (will be added to the {@link Table} if not exists)
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedOptions<C> overrideName(SerializableBiConsumer<C, IN> setter, String columnName);
	
	<IN> ImportedEmbedOptions<C> exclude(SerializableFunction<C, IN> getter);
	
	<IN> ImportedEmbedOptions<C> exclude(SerializableBiConsumer<C, IN> setter);
	
}
