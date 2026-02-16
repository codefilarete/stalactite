package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersisterPostgreSQLTest extends PersisterITTest {
	
	private static final DataSource DATASOURCE = new PostgreSQLTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new PostgreSQLDatabaseHelper();
	}
	
	@Override
	Dialect createDialect() {
		return PostgreSQLDialectBuilder.defaultPostgreSQLDialect();
	}
}
