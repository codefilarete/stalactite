package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.MariaDBDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.MariaDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
import org.junit.jupiter.api.BeforeEach;

/**
 * Same as {@link InsertExecutorAutoGeneratedKeysITTest} but dedicated to Derby because of its implementation of generated keys.
 *
 * @author Guillaume Mary
 */
public class InsertExecutorAutoGeneratedKeysMariaDBTest extends InsertExecutorAutoGeneratedKeysITTest {
	
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
	@BeforeEach
	void createDialect() {
		dialect = MariaDBDialectBuilder.defaultMariaDBDialect();
	}
}