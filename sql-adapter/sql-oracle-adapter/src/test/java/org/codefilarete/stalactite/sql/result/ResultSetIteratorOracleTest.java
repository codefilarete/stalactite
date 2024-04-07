package org.codefilarete.stalactite.sql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorOracleTest extends ResultSetIteratorITTest {
	
	private static final DataSource DATASOURCE = new OracleTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new OracleDatabaseHelper();
	}
}
