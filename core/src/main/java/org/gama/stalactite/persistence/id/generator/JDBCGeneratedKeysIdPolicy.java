package org.gama.stalactite.persistence.id.generator;

import java.sql.PreparedStatement;
import java.util.Map;

import org.gama.sql.dml.GeneratedKeysReader;

/**
 * Marker interface for identifier generators which get identifiers from JDBC insert statements thanks to
 * {@link java.sql.PreparedStatement#getGeneratedKeys()}.
 * 
 * Shouldn't be combined with {@link BeforeInsertIdPolicy} nor {@link AlreadyAssignedIdPolicy}.
 * 
 * @author Guillaume Mary
 */
public interface JDBCGeneratedKeysIdPolicy<I> extends IdAssignmentPolicy {
	
	/**
	 * Must return the identifier from the given {@link Map}
	 * @param generatedKeys the keys read from the {@link java.sql.ResultSet} given by {@link PreparedStatement#getGeneratedKeys()} mapped by column name
	 * @return thought to be not null
	 */
	I getId(Map<String, Object> generatedKeys);
	
	/**
	 * Gives the objet that will collect the generated keys from the {@link java.sql.ResultSet} given by {@link PreparedStatement#getGeneratedKeys()}
	 * 
	 * @return not null
	 */
	GeneratedKeysReader getGeneratedKeysReader();
}
