package org.gama.stalactite.sql.dml;

import java.util.function.Predicate;

import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import org.codefilarete.tool.exception.Exceptions;
import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationMySQLTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new MySQLEmbeddableDataSource(3406);
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto WRITE";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        // MySQL throws an exception on query cancelation (https://mariadb.com/kb/en/library/multi-threading-and-statementcancel/), we check it.
        return t -> Exceptions.findExceptionInCauses(t, MySQLStatementCancelledException.class, "Statement cancelled due to client request") != null;
    }
}
