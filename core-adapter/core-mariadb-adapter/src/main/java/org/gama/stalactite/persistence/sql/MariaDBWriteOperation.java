package org.gama.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.Retryer.RetryException;
import org.gama.lang.function.ThrowingBiFunction;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * MariaDB's implementation of {@link WriteOperation} to retry statement execution because MariaDB InnoDB has a poor behavior of insertion concurrency
 * 
 * @author Guillaume Mary
 */
public class MariaDBWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	private final InnoDBLockRetryer retryer;
	
	private final ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider;
	
	public MariaDBWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener,
								 InnoDBLockRetryer retryer, ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider) {
		super(sqlGenerator, connectionProvider, rowCountListener);
		this.retryer = retryer;
		this.statementProvider = statementProvider;
	}
	
	public MariaDBWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener, InnoDBLockRetryer retryer) {
		this(sqlGenerator, connectionProvider, rowCountListener, retryer, Connection::prepareStatement);
	}
	
	@Override
	protected void prepareStatement(Connection connection) throws SQLException {
		this.preparedStatement = statementProvider.apply(connection, getSQL());
	}
	
	/**
	 * Overriden to :
	 * - call executeUpdate() instead of executeLargeUpdate() because MariaDB client 1.3.4 doesn't support it
	 * - call retryer to prevent exception due to key lock on insert
	 */
	@Override
	protected long doExecuteUpdate() throws SQLException {
		try {
			return (long) retryer.execute(preparedStatement::executeUpdate, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	/**
	 * Overriden to :
	 * - call executeBatch() instead of executeLargeBatch() because MariaDB client 1.3.4 doesn't support it
	 * - call retryer to prevent exception due to key lock on insert
	 */
	@Override
	protected long[] doExecuteBatch() throws SQLException {
		int[] updatedRowCounts;
		try {
			updatedRowCounts = retryer.execute(preparedStatement::executeBatch, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
		long[] updatedRowCountsAsLong = new long[updatedRowCounts.length];
		for (int i = 0; i < updatedRowCounts.length; i++) {
			updatedRowCountsAsLong[i] = updatedRowCounts[i];
		}
		return updatedRowCountsAsLong;
	}
}
