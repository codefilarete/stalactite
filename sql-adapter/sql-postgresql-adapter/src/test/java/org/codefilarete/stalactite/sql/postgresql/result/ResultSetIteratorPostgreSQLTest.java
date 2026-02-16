package org.codefilarete.stalactite.sql.postgresql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorPostgreSQLTest extends ResultSetIteratorITTest {
	
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
