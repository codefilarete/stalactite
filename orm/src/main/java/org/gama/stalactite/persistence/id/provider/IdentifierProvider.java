package org.gama.stalactite.persistence.id.provider;

import org.gama.stalactite.persistence.id.PersistableIdentifier;

/**
 * Interface for giving a {@link PersistableIdentifier} that is not already used (unique), and so can be inserted in database without
 * breaking unicity constraint.
 * 
 * Expected to be thread-safe. If it's not the case, mention it.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface IdentifierProvider<T> {
	
	PersistableIdentifier<T> giveNewIdentifier();
	
}
