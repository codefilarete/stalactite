package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public class PersisterDerbyTest extends PersisterITTest {
	
    @Override
	@BeforeEach
    void createDataSource() {
        dataSource = new DerbyInMemoryDataSource();
    }
}
