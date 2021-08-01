package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SelectExecutorPostgreSQLTest extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new PostgreSQLEmbeddedDataSource(5431);
	}
}
