package org.gama.stalactite.sql.result;

import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorMySQLTest extends ResultSetIteratorITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new MySQLEmbeddableDataSource(3406);
    }
}
