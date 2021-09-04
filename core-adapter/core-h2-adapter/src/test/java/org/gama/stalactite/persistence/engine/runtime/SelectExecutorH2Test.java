package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SelectExecutorH2Test extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new H2InMemoryDataSource();
	}
}
