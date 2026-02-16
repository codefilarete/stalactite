package org.codefilarete.stalactite.sql.hsqldb.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorHSQLDBTest extends ResultSetIteratorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
}
