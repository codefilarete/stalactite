package org.codefilarete.stalactite.id;

import javax.annotation.Nonnull;

import org.codefilarete.stalactite.engine.runtime.Persister;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;

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
public interface StatefulIdentifier<T> {
	
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
