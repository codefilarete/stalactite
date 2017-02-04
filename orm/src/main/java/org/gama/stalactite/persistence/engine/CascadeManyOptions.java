package org.gama.stalactite.persistence.engine;

import java.util.function.BiConsumer;

import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
public interface CascadeManyOptions<T extends Identified, I extends StatefullIdentifier, O extends Identified> {
	
	IFluentMappingBuilder<T, I> mappedBy(BiConsumer<O, T> reverseLink);
}
