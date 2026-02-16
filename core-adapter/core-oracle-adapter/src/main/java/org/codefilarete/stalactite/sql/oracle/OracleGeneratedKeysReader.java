package org.codefilarete.stalactite.sql.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;

/**
 * Implementation dedicated to Oracle
 * 
 * @author Guillaume Mary
 */
public class OracleGeneratedKeysReader extends GeneratedKeysReader<Integer> {
	
	public OracleGeneratedKeysReader(String keyName) {
		super(keyName, DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
	}
	
	/**
	 * Overridden to read generated key from its column number : 1
	 * 
	 * @param rs {@link ResultSet} to extract generated key from
	 * @return value found under column number 1
	 * @throws SQLException if any error occurs while reading value
	 */
	@Override
	protected Integer readKey(ResultSet rs) throws SQLException {
		// Even if Oracle requires that we give it the Column name to retrieve in the PreparedStatement, it doesn't
		// support to read it by name (see OracleDatabaseSettings.OracleWriteOperationFactory)
		return rs.getInt(1);
	}
}
