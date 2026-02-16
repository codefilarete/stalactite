package org.codefilarete.stalactite.sql.h2.test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.tool.exception.Exceptions;

/**
 * @author Guillaume Mary
 */
public class H2DatabaseHelper extends DatabaseHelper {
	
	@Override
	public void clearDatabaseSchema(Connection connection) {
		Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute("DROP ALL OBJECTS");
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		
	}
}
