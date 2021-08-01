package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SelectExecutorHSQLDBTest extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new HSQLDBInMemoryDataSource();
	}
}
