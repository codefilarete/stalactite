package org.codefilarete.stalactite.sql.derby.result;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.result.ResultSetIteratorITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.derby.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.derby.test.DerbyInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorDerbyTest extends ResultSetIteratorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
	}
}
