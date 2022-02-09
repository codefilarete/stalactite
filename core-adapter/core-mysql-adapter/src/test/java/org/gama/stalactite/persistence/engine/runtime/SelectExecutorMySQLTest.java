package org.codefilarete.stalactite.persistence.engine.runtime;

import org.codefilarete.stalactite.sql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SelectExecutorMySQLTest extends SelectExecutorITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new MySQLEmbeddableDataSource(3406);
	}
}
