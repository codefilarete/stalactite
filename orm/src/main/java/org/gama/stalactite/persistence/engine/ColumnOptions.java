package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<T, I> {
	
	/**
	 * Defines the column as the identifier of the entity.
	 * 
	 * @param identifierPolicy an {@link IdentifierPolicy}
	 * @return the enclosing {@link IFluentMappingBuilder}
	 */
	IFluentMappingBuilder<T, I> identifier(IdentifierPolicy identifierPolicy);
}
