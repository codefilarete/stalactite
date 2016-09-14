package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;

/**
 * @author Guillaume Mary
 */
interface ColumnOptions<T, I> {
	
	IFluentMappingBuilder<T, I> identifier(IdentifierPolicy identifierPolicy);
}
