package org.codefilarete.stalactite.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;

/**
 * Implementation dedicated to MariaDB
 * 
 * @author Guillaume Mary
 */
public class MariaDBGeneratedKeysReader extends GeneratedKeysReader<Integer> {
	
	public MariaDBGeneratedKeysReader() {
		// For MariaDB generated key column is always named "insert_id" (can be ".insert_id" too)
		// but we prefer to read it through its number : see overriding of readKey()
		super("insert_id", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER);
	}
	
	/**
	 * Overridden to read generated key from its column number : 1
	 * @param rs {@link ResultSet} to extract generated key from
	 * @return value found under column number 1
	 * @throws SQLException if any error occurs while reading value
	 */
	@Override
	protected Integer readKey(ResultSet rs) throws SQLException {
		return rs.getInt(1);
	}
}
