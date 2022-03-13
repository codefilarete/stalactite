package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextHSQLDBTest extends PersistenceContextITTest {

	@Override
	protected DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
	
	@Override
	protected Dialect createDialect() {
		return new HSQLDBDialect();
	}
}