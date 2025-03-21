package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.MySQLDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextMySQLTest extends PersistenceContextITTest {
	
	private static final DataSource DATASOURCE = new MySQLTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new MySQLDatabaseHelper();
	}
	
	@Override
	protected Dialect createDialect() {
		return MySQLDialectBuilder.defaultMySQLDialect();
	}
}