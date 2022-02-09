package org.codefilarete.stalactite.sql.result;

import org.codefilarete.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorMariaDBTest extends ResultSetIteratorITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new MariaDBEmbeddableDataSource(3406);
    }
}
