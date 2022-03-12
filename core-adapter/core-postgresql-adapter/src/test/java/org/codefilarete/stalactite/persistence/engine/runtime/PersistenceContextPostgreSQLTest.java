package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.PostgreSQLDialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.PostgreSQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextPostgreSQLTest extends PersistenceContextITTest {

	private static final DataSource DATASOURCE = new PostgreSQLTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new PostgreSQLDatabaseHelper();
	}
	
	@Override
	protected Dialect createDialect() {
		return new PostgreSQLDialect();
	}
}