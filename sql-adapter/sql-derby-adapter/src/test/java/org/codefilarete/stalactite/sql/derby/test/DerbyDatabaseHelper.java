package org.codefilarete.stalactite.sql.derby.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class DerbyDatabaseHelper extends DatabaseHelper {
	
	/**
	 * Overridden to select tables from "APP" schema which is default one.
	 */
	@Override
	protected ResultSetIterator<String> lookupTables(Connection connection) {
		// default Derby schema is "APP" so we query tables for it else we'll try to delegate system tables  
		ResultSet tables;
		try {
			tables = connection.getMetaData().getTables(null, "APP", null, new String[] { "TABLE" });
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
