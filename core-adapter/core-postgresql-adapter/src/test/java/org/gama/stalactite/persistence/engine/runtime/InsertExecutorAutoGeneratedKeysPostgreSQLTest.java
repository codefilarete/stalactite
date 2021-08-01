package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.persistence.sql.PostgreSQLDialect;
import org.gama.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * Same as {@link InsertExecutorAutoGeneratedKeysITTest} but dedicated to Derby because of its implementation of generated keys.
 *
 * @author Guillaume Mary
 */
public class InsertExecutorAutoGeneratedKeysPostgreSQLTest extends InsertExecutorAutoGeneratedKeysITTest {
	
	@Override
	@BeforeEach
	void createDialect() {
		dialect = new PostgreSQLDialect();
	}

	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new PostgreSQLEmbeddedDataSource(5431);
	}
}