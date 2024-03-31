package org.codefilarete.stalactite.sql.statement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Predicate;

import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class SQLOperationSQLiteTest extends SQLOperationITTest {
	
    @Override
	public DataSource giveDataSource() {
        return new SQLiteInMemoryDataSource();
	}
	
	protected void lockTable(Connection lockingConnection) throws SQLException {
		lockingConnection.setSavepoint();
		lockingConnection.prepareStatement(giveLockStatement()).execute();
	}
	
	@Test
	@Override
	void cancel() {
		// we do nothing here because I couldn't find a way to check that SQLite effectively canceled the operation :
		// no exception is thrown, no sql command can be done to check it. The only operation available is a low level
		// one in the native library called "is_interrupted" but is not exposed in Java.
		// (https://github.com/sqlite/sqlite/blob/69dbd7a4e772e5f5a59409c45f29dbb038a06adc/src/sqlite.h.in#L2761)
		// Moreover SQLite doesn't apply any lock (except on the whole database) so the algorithm in place in the test
		// is not appropriate : we should have a long-running request to be canceled instead of a lock.
		// With all of this, it's very difficult to test SQLOperation.cancel() for SQLite, we hope it works
		// and trust SQLite to implement PreparedStatement.cancel() (which the method called by SQLOperation.cancel())
		// super.cancel();
	}
	
	@Override
    String giveLockStatement() {
        return "PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE; insert into Toto(id) values(42);";
    }
	
    @Override
    Predicate<Throwable> giveCancelOperationPredicate() {
        return Objects::isNull;
    }
}
