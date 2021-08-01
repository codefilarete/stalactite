package org.gama.stalactite.sql.dml;

import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class SQLOperationDerbyTest extends SQLOperationITTest {
    
    @Override
    @BeforeEach
    void createDataSource() {
        super.dataSource = new DerbyInMemoryDataSource();
    }

    @Override
    String giveLockStatement() {
        return "lock table Toto in EXCLUSIVE MODE";
    }

    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }

	@Override
	protected void doCancel(ReadOperation<Integer> testInstance) throws SQLException {
		// TODO this must be moved to DerbyDialect : Dialect is the right place to adapt statement according to database particularities, but for now
		// ReadOperation instanciation is not delegated to Dialect, therefore this is not possible. Delegating SQLOperations to Dialect needs some
		// rework
		final EmbedConnection conn = testInstance.getPreparedStatement().getConnection().unwrap(EmbedConnection.class);
		conn.cancelRunningStatement();
	}
}
