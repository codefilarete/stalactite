package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * Same as {@link InsertExecutorAutoGeneratedKeysITTest} but dedicated to Derby because of its implementation of generated keys.
 *
 * @author Guillaume Mary
 */
public class InsertExecutorAutoGeneratedKeysHSQLDBTest extends InsertExecutorAutoGeneratedKeysITTest {
	
	@Override
	protected DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	void createDialect() {
		dialect = new HSQLDBDialect();
	}
}