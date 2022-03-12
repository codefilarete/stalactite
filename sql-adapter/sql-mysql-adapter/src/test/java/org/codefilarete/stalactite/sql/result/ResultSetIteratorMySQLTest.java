package org.codefilarete.stalactite.sql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorMySQLTest extends ResultSetIteratorITTest {
	
	private static final DataSource DATASOURCE = new MySQLTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
}
