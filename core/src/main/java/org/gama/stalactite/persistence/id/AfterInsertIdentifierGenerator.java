package org.gama.stalactite.persistence.id;

/**
 * Marker interface for identifier generators which get identifiers from JDBC insert statements thanks to
 * {@link java.sql.PreparedStatement#getGeneratedKeys()}.
 * Shouldn't be combined with {@link BeforeInsertIdentifierGenerator} not {@link AutoAssignedIdentifierGenerator}.
 *
 * @author Guillaume Mary
 */
public interface AfterInsertIdentifierGenerator extends IdentifierGenerator {
	
	// TODO: add contract methods
}
