package org.codefilarete.stalactite.sql.oracle.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleTestDataSourceSelector;

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
