package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;

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
		return PostgreSQLDialectBuilder.defaultPostgreSQLDialect();
	}
}
