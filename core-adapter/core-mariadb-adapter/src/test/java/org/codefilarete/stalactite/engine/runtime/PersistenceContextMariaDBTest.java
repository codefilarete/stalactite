package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.mariadb.MariaDBDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.mariadb.test.MariaDBDatabaseHelper;
import org.codefilarete.stalactite.sql.mariadb.test.MariaDBTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextMariaDBTest extends PersistenceContextITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new MariaDBDatabaseHelper();
	}
	
	@Override
	protected Dialect createDialect() {
		return MariaDBDialectBuilder.defaultMariaDBDialect();
	}
}
