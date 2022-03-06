package org.codefilarete.stalactite.sql.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class DerbyDatabaseHelper extends DatabaseHelper {
	
	/**
	 * Overriden to select tables from "APP" schema which is default one.
	 */
	@Override
	protected ResultSetIterator<String> lookupTables(Connection connection) {
		// default Derby schema is "APP" so we query tables for it else we'll try to delegate system tables  
		ResultSet tables = null;
		try {
			tables = connection.getMetaData().getTables(null, "APP", null, null);
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		ResultSetIterator<String> tablesIterator = new ResultSetIterator<String>(tables) {
			@Override
			public String convert(ResultSet resultSet) throws SQLException {
				int table_name = resultSet.findColumn("TABLE_NAME");
				return resultSet.getString(table_name);
			}
		};
		return tablesIterator;
	}
}