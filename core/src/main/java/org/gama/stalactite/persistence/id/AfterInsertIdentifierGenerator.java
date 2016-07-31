package org.gama.stalactite.persistence.id;

import java.util.Map;

/**
 * Marker interface for identifier generators which get identifiers from JDBC insert statements thanks to
 * {@link java.sql.PreparedStatement#getGeneratedKeys()}.
 * This kind of generator is not supported since it's incompatible with effective jdbc batching.
 * 
 * Shouldn't be combined with {@link BeforeInsertIdentifierGenerator} not {@link AutoAssignedIdentifierGenerator}.
 *
 * @author Guillaume Mary
 * @deprecated Unsupported
 */
public interface AfterInsertIdentifierGenerator extends IdentifierGenerator {
	
	Object get(Map<String, Object> generatedKeys);
}
