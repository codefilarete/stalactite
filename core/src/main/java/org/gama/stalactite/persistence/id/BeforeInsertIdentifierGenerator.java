package org.gama.stalactite.persistence.id;

/**
 * Marker interface for identifier generators that must be called before insert statement building.
 * Shouldn't be combined with {@link AfterInsertIdentifierGenerator} not {@link AutoAssignedIdentifierGenerator}.
 * 
 * @author Guillaume Mary
 */
public interface BeforeInsertIdentifierGenerator extends IdentifierGenerator {
	
	// Nothing special (marker interface)
}
