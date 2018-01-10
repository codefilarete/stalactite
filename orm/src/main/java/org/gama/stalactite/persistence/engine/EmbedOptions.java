package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderEmbedOptions;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * Contract for configuring embedded object
 * 
 * @author Guillaume Mary
 */
public interface EmbedOptions<T extends Identified, I extends StatefullIdentifier, O> {
	
	IFluentMappingBuilderEmbedOptions<T, I, O> overrideName(SerializableFunction<O, ?> function, String columnName);
}