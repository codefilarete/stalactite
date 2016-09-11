package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.PersistenceMapper.IdentifierPolicy;

/**
 * @author Guillaume Mary
 */
interface ColumnOptions<T> {
	
	IPersistenceMapper<T> identifier(IdentifierPolicy identifierPolicy);
}
