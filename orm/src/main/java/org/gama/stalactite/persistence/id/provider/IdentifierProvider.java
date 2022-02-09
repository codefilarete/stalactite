package org.codefilarete.stalactite.persistence.id.provider;

/**
 * Interface for giving an identifier that is not already used (a unique value), and so can be inserted in database without
 * breaking unicity constraint. Unicity is expected to be at least in the context of current instance but can be wider.
 * 
 * Expected to be thread-safe. If it's not the case, mention it.
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface IdentifierProvider<T> {
	
	T giveNewIdentifier();
	
}
