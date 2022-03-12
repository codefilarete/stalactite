package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.MySQLDialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLDatabaseHelper;
import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;
import org.junit.jupiter.api.BeforeEach;

/**
 * Same as {@link InsertExecutorAutoGeneratedKeysITTest} but dedicated to Derby because of its implementation of generated keys.
 *
 * @author Guillaume Mary
 */
public class InsertExecutorAutoGeneratedKeysMySQLTest extends InsertExecutorAutoGeneratedKeysITTest {
	
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
	@BeforeEach
	void createDialect() {
		dialect = new MySQLDialect();
	}
}