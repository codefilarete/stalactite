package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class SelectExecutorPostgreSQLTest extends SelectExecutorITTest {
	
	private static final DataSource DATASOURCE = new PostgreSQLTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new PostgreSQLDatabaseHelper();
	}
}
