package org.codefilarete.stalactite.sql.mysql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.mysql.test.MySQLDatabaseHelper;
import org.codefilarete.stalactite.sql.mysql.test.MySQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorMySQLTest extends ResultSetIteratorITTest {
	
	private static final DataSource DATASOURCE = new MySQLTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new MySQLDatabaseHelper();
	}
}
