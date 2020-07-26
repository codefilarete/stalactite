package org.gama.stalactite.persistence.id.manager;

import javax.annotation.Nonnull;

import org.gama.stalactite.persistence.engine.runtime.Persister;

/**
 * A marker for identifier that are wrapped into a simple class.
 * The surrogate will be persisted, not the whole instances of this class.
 * 
 * Mainly introduced to manage {@link AlreadyAssignedIdentifierManager} and the need to determine if an instance is persisted or not
 * (see {@link Persister#persist(Object)}.
 * 
 * @param <T> the real type of the identifier
 * @author Guillaume Mary
 */
public interface StatefullIdentifier<T> {
	
	/**
	 * Returns the value to persist
	 * @return the value to persist, obviously not null
	 */
	@Nonnull
	T getSurrogate();
	
	/**
	 * Gives the persistence state of this identifier: true means that this identifier is used by a database row and commited in the database.
	 * False means it's not (which means not used at all or not yet commited).
	 *
	 * @return the persistence state of this identifier
	 */
	boolean isPersisted();
	
	/**
	 * Method called after entity insertion to mark this instance as persisted
	 */
	void setPersisted();
}
