package org.codefilarete.stalactite.sql.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.H2DatabaseHelper;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorH2Test extends ResultSetIteratorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new H2InMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new H2DatabaseHelper();
	}
}
