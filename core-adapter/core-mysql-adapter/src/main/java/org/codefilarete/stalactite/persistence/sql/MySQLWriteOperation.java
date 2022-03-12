package org.codefilarete.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.Retryer.RetryException;
import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.dml.SQLExecutionException;
import org.codefilarete.stalactite.sql.dml.SQLStatement;
import org.codefilarete.stalactite.sql.dml.WriteOperation;

/**
 * MySQL's implementation of {@link WriteOperation} to retry statement execution because MySQL InnoDB has a poor behavior of insertion concurrency
 * 
 * @author Guillaume Mary
 */
public class MySQLWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	private final InnoDBLockRetryer retryer;
	
	private final ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider;
	
	public MySQLWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener,
							   InnoDBLockRetryer retryer, ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider) {
		super(sqlGenerator, connectionProvider, rowCountListener);
		this.retryer = retryer;
		this.statementProvider = statementProvider;
	}
	
	public MySQLWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener, InnoDBLockRetryer retryer) {
		this(sqlGenerator, connectionProvider, rowCountListener, retryer, Connection::prepareStatement);
	}
	
	@Override
	protected void prepareStatement(Connection connection) throws SQLException {
		this.preparedStatement = statementProvider.apply(connection, getSQL());
	}
	
	@Override
	protected long[] doExecuteBatch() throws SQLException {
		try {
			return retryer.execute(preparedStatement::executeLargeBatch, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
	
	@Override
	protected long doExecuteUpdate() throws SQLException {
		try {
			return retryer.execute(preparedStatement::executeLargeUpdate, getSQL());
		} catch (RetryException e) {
			throw new SQLExecutionException(getSQL(), e);
		}
	}
}
