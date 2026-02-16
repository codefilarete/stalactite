package org.codefilarete.stalactite.sql.hsqldb.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDatabaseHelper extends DatabaseHelper {
	
	@Override
	public void clearDatabaseSchema(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			statement.execute("DROP SCHEMA PUBLIC CASCADE");
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
}
