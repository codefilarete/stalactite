package org.codefilarete.stalactite.sql.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class MariaDBDatabaseHelper extends DatabaseHelper {
	
	/**
	 * Overridden to select tables of connection catalog and schema
	 */
	@Override
	protected ResultSetIterator<String> lookupTables(Connection connection) {
		ResultSet tables;
		try {
			tables = connection.getMetaData().getTables(connection.getCatalog(), connection.getSchema(), null, new String[] { "TABLE" });
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
