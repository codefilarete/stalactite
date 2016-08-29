package org.gama.stalactite.persistence.id.generator;

import java.util.Map;

/**
 * Marker interface for identifier generators which get identifiers from JDBC insert statements thanks to
 * {@link java.sql.PreparedStatement#getGeneratedKeys()}.
 * 
 * Shouldn't be combined with {@link BeforeInsertIdentifierGenerator} nor {@link AutoAssignedIdentifierGenerator}.
 * 
 * @author Guillaume Mary
 */
public interface AfterInsertIdentifierGenerator extends IdentifierGenerator {
	
	Object get(Map<String, Object> generatedKeys);
}
