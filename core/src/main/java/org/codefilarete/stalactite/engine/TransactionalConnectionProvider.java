package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.tool.exception.Exceptions;

/**
 * 
 * 
 * @author Guillaume Mary
 */
public class TransactionalConnectionProvider implements ConnectionProvider, SeparateTransactionExecutor {
	
	private final CurrentThreadConnectionProvider jdbcConnectionProvider;
	
	public TransactionalConnectionProvider(DataSource dataSource) {
		this.jdbcConnectionProvider = new CurrentThreadConnectionProvider(dataSource);
	}
	
	@Override
	public Connection giveConnection() {
		return this.jdbcConnectionProvider.giveConnection();
	}
	
	/**
	 * Implementation based on a {@link Savepoint} on current {@link Connection} to execute given operation.
	 * 
	 * @param jdbcOperation a sql operation that will call {@link #giveConnection()} to execute its statements.
	 */
	@Override
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		Connection currentConnection = giveConnection();
		Savepoint savepoint = null;
		try {
			savepoint = currentConnection.setSavepoint();
			jdbcOperation.execute();
		} catch (Exception e) {
			if (savepoint != null) {
				try {
					currentConnection.rollback(savepoint);
					throw Exceptions.asRuntimeException(e);
				} catch (SQLException e1) {
					throw Exceptions.asRuntimeException(e1);
				}
			} else {
				// else savepoint can't be created, let's throw an exception
				throw Exceptions.asRuntimeException(e);
			}
		} 
		try {
			currentConnection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
