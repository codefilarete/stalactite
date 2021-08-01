package org.gama.stalactite.sql.result;

import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorHSQLDBTest extends ResultSetIteratorITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new HSQLDBInMemoryDataSource();
    }
}
