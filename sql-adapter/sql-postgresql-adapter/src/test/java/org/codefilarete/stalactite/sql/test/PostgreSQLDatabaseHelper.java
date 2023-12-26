package org.codefilarete.stalactite.sql.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLDatabaseHelper extends DatabaseHelper{
	
	/**
	 * Overridden to drop table of current schema, else tables of every schema are selected for deletion, even those of system schema, which throws
	 * an error.
	 * @param connection connection used for table lookup
	 * @return table names as an {@link java.util.Iterator}
	 */
	@Override
	protected ResultSetIterator<String> lookupTables(Connection connection) {
		ResultSet tablesResultSet;
		try {
			// Looking for current schema
			PreparedStatement getCurrentSchemaStatement = connection.prepareStatement("select current_schema()");
			ResultSet resultSet = getCurrentSchemaStatement.executeQuery();
			resultSet.next();
			String currentSchema = resultSet.getString(1);
			
			// Specifying "TABLE" as type else index are also returned
			tablesResultSet = connection.getMetaData().getTables(null, currentSchema, null, new String[] { "TABLE" });
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
}
