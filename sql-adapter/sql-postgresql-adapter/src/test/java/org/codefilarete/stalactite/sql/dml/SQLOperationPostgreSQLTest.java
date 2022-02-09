package org.codefilarete.stalactite.sql.dml;

import java.util.function.Predicate;

import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.util.PSQLException;

/**
 * @author Guillaume Mary
 */
class SQLOperationPostgreSQLTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new PostgreSQLEmbeddedDataSource(5431);
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto NOWAIT";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        // PostgreSQL throws an exception on query cancelation
        return t -> Exceptions.findExceptionInCauses(t, PSQLException.class, "ERROR: canceling statement due to user request") != null;
    }
}
