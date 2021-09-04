package org.gama.stalactite.sql.result;

import org.gama.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorH2Test extends ResultSetIteratorITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new H2InMemoryDataSource();
    }
}
