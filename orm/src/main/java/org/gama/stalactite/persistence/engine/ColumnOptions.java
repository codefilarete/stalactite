package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<T extends Identified, I extends StatefullIdentifier> {
	
	/**
	 * Defines the column as the identifier of the entity.
	 * 
	 * @param identifierPolicy an {@link IdentifierPolicy}
	 * @return the enclosing {@link IFluentMappingBuilder}
	 */
	IFluentMappingBuilder<T, I> identifier(IdentifierPolicy identifierPolicy);
}
