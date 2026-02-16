package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2InMemoryDataSource;

/**
 * @author Guillaume Mary
 */
class SelectExecutorH2Test extends SelectExecutorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new H2InMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new H2DatabaseHelper();
	}
}
