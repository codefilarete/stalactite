package org.gama.stalactite.sql.dml;

import java.sql.SQLTransientException;
import java.util.function.Predicate;

import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationMariaDBTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new MariaDBEmbeddableDataSource(3406);
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto WRITE";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        // MySQL throws an exception on query cancelation (https://mariadb.com/kb/en/library/multi-threading-and-statementcancel/), we check it.
        return t -> Exceptions.findExceptionInCauses(t, SQLTransientException.class, "Query execution was interrupted") != null;
    }
}
