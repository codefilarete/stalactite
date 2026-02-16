package org.codefilarete.stalactite.sql.sqlite.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.sqlite.test.SQLiteInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorSQLiteTest extends ResultSetIteratorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new SQLiteInMemoryDataSource();
	}
}
