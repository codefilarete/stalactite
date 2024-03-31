package org.codefilarete.stalactite.sql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorSQLiteTest extends ResultSetIteratorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new SQLiteInMemoryDataSource();
	}
}
