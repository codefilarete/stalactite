package org.codefilarete.stalactite.sql.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class DatabaseHelper {
	
	public void clearDatabaseSchema(Connection connection) {
		ResultSetIterator<String> tablesIterator = lookupTables(connection);
		dropTable(connection, tablesIterator);
	}
	
	/**
	 * @param connection connection used for table lookup
	 * @return table names as an {@link java.util.Iterator}
	 */
	protected ResultSetIterator<String> lookupTables(Connection connection) {
		ResultSet tablesResultSet;
		try {
			tablesResultSet = connection.getMetaData().getTables(null, null, null, new String[] { "TABLE" });
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		return new ResultSetIterator<String>(tablesResultSet) {
			@Override
			public String convert(ResultSet resultSet) throws SQLException {
				int table_name = resultSet.findColumn("TABLE_NAME");
				return resultSet.getString(table_name);
			}
		};
	}
	
	public void dropTable(Connection connection, Iterator<String> tablesIterator) {
		tablesIterator.forEachRemaining(tableName -> {
			try {
				Statement statement = connection.createStatement();
				statement.execute("drop table " + tableName);
			} catch (SQLException e) {
				throw new RuntimeException("Error while trying to drop table " + tableName, e);
			}
		});
	}
	
}
