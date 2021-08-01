package org.gama.stalactite.sql.dml;

import java.util.Objects;
import java.util.function.Predicate;

import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationHSQLDBTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new HSQLDBInMemoryDataSource();
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto WRITE";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }
}
