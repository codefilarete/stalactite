package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.DataSourceConnectionProvider;

/**
 * 
 * 
 * @author Guillaume Mary
 */
public class TransactionalConnectionProvider implements ConnectionProvider, SeparateTransactionExecutor {
	
	private final DataSourceConnectionProvider jdbcConnectionProvider;
	
	public TransactionalConnectionProvider(DataSource dataSource) {
		this.jdbcConnectionProvider = new DataSourceConnectionProvider(dataSource);
	}
	
	@Nonnull
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
