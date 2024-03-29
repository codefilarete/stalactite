package org.codefilarete.stalactite.sql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.MariaDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorMariaDBTest extends ResultSetIteratorITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	protected DatabaseHelper giveDatabaseHelper() {
		return new MariaDBDatabaseHelper();
	}
}
