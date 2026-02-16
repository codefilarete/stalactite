package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;

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
		return HSQLDBDialectBuilder.defaultHSQLDBDialect();
	}
}
