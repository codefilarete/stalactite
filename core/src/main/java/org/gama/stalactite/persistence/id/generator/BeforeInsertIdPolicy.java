package org.gama.stalactite.persistence.id.generator;

/**
 * Marker interface for identifier generators that must be called before insert statement building.
 * Shouldn't be combined with {@link JDBCGeneratedKeysIdPolicy} not {@link AlreadyAssignedIdPolicy}.
 * 
 * @param <I> the type of the generated identifier, not expected do be complex
 * @author Guillaume Mary
 */
public interface BeforeInsertIdPolicy<I> extends IdAssignmentPolicy {
	
	I generate();
}
