package org.codefilarete.stalactite.sql.test;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public abstract class DatabaseIntegrationTest {
	
	protected ConnectionProvider connectionProvider;
	
	@BeforeEach
	protected void init() {
		setConnectionProvider();
		// we clean database schema before test instead of after because it may be not correctly cleaned when test fails
		clearDatabaseSchema();
	}
	
	protected void setConnectionProvider() {
		DataSource dataSource = giveDataSource();
		try {
			this.connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	protected void clearDatabaseSchema() {
		giveDatabaseHelper().clearDatabaseSchema(connectionProvider.giveConnection());
	}
	
	protected abstract DataSource giveDataSource();
	
	protected DatabaseHelper giveDatabaseHelper() {
		return new DatabaseHelper();
	}
}
