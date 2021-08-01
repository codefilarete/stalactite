package org.gama.stalactite.persistence.engine.runtime;

import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SelectExecutorMariaDBTest extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new MariaDBEmbeddableDataSource(3406);
	}
}
