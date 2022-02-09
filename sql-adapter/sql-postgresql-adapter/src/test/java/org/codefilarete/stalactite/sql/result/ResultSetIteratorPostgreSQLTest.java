package org.codefilarete.stalactite.sql.result;

import org.codefilarete.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class ResultSetIteratorPostgreSQLTest extends ResultSetIteratorITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new PostgreSQLEmbeddedDataSource(5431);
    }
}
