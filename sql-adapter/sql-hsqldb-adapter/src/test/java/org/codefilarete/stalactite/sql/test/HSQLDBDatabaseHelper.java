package org.codefilarete.stalactite.sql.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDatabaseHelper extends DatabaseHelper {
	
	@Override
	public void clearDatabaseSchema(Connection connection) {
		try {
			Statement statement = connection.createStatement();
			statement.execute("TRUNCATE SCHEMA public AND commit");
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
}
