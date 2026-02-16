package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class SelectExecutorHSQLDBTest extends SelectExecutorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
}
