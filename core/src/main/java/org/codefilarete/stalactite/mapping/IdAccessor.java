package org.codefilarete.stalactite.mapping;

import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;

/**
 * Interface for general access to the identifier of an entity
 * 
 * @author Guillaume Mary
 */
public interface IdAccessor<C, I> {
	
	/**
	 * Gets an entity identifier.
	 * Used for SQL write orders for instance, in where clause, to target the right entity
	 * 
	 * @param c any entity
	 * @return the entity identifier
	 */
	I getId(C c);
	
	/**
	 * Sets entity identifier.
	 * Used on very first time persistence of the entity in conjonction with {@link IdentifierInsertionManager}
	 * 
	 * @param c an entity
	 * @param identifier the generated identifier
	 */
	void setId(C c, I identifier);
	
}
