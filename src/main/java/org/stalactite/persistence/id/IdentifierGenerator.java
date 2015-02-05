package org.stalactite.persistence.id;

import java.io.Serializable;

import org.stalactite.persistence.engine.PersistenceContext;

/**
 * @author mary
 */
public interface IdentifierGenerator {
	
	Serializable generate(PersistenceContext context, Object persistentBean);
}
