package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
class SelectExecutorOracleTest extends SelectExecutorITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new OracleEmbeddableDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new OracleDatabaseHelper();
	}
}