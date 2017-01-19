package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;

/**
 * @author Guillaume Mary
 */
interface ColumnOptions<T extends Identified, I extends StatefullIdentifier> {
	
	IFluentMappingBuilder<T, I> identifier(IdentifierPolicy identifierPolicy);
}
