package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.sqlite.test.SQLiteInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class SelectExecutorSQLiteTest extends SelectExecutorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new SQLiteInMemoryDataSource();
	}
}
