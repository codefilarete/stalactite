package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * Same as {@link InsertExecutorAutoGeneratedKeysITTest} but dedicated to Derby because of its implementation of generated keys.
 *
 * @author Guillaume Mary
 */
public class InsertExecutorAutoGeneratedKeysHSQLDBTest extends InsertExecutorAutoGeneratedKeysITTest {
	
	@Override
	@BeforeEach
	void createDialect() {
		dialect = new HSQLDBDialect();
	}

	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new HSQLDBInMemoryDataSource();
	}
}