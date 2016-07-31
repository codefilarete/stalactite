package org.gama.stalactite.persistence.id;

import org.gama.stalactite.persistence.id.provider.IdentifierProvider;

/**
 * A decorator for bean identifier.
 * The surrogate must be persisted, not the instances of this class.
 * 
 * Main purpose is to get ride of attach/detach principle that's in JPA with the aid of {@link IdentifierProvider}
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public interface Identifier<T> {
	
	T getSurrogate();
	
	/**
	 * Gives the persistence state of this identifier: true means that this identifier is used by a database row and commited in the database.
	 * False means it's not (which means not used at all or not yet commited).
	 * 
	 * @return this implementation always returns true. See {@link PersistableIdentifier} for change made possible.
	 */
	boolean isPersisted();
}
