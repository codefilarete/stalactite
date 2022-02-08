package org.gama.stalactite.sql;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.tool.Retryer;
import org.codefilarete.tool.Retryer.RetryException;

/**
 * Implementation that gives connections from an underlying {@link DataSource}.
 * {@link #giveConnection()} gives the current {@link Connection} as a Thread point of view. It may be detached from it thanks to
 * {@link #releaseConnection()}.
 * 
 * @author Guillaume Mary
 */
public class DataSourceConnectionProvider implements ConnectionProvider {
	
	private final DataSource dataSource;
	
	private final ThreadLocal<Connection> currentConnection = new ThreadLocal<>();
	private final ClosedConnectionRetryer closedConnectionRetryer;
	
	public DataSourceConnectionProvider(DataSource dataSource) {
		this(dataSource, 5);
	}
	
	public DataSourceConnectionProvider(DataSource dataSource, int connectionOpeningRetryMaxCount) {
		this.dataSource = dataSource;
		// Since Retryer is expected to be Thread-safe we instanciate is once
		this.closedConnectionRetryer = new ClosedConnectionRetryer(connectionOpeningRetryMaxCount);
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	/**
	 * Implemented to provide current {@link Thread} {@link Connection}. If one doesn't exist then it is filled and will be provided on next calls.
	 * 
	 * @return current Thread {@link Connection}
	 * @see #releaseConnection()
	 * @see #fillCurrentConnection() 
	 */
	@Nonnull
	@Override
	public Connection giveConnection() {
		Connection connection = this.currentConnection.get();
		try {
			if (connection == null) {
				return fillCurrentConnection();
			} else if (connection.isClosed()) {
				this.currentConnection.remove();	// closed connection has no interest
				return fillCurrentConnection();
			} else {
				return connection;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Attaches a {@link Connection} to current {@link Thread}
	 * {@link Connection} is set auto-commit to false to enable transaction mode because it better suits usual ORM usage.
	 * 
	 * @return the attached {@link Connection}
	 */
	public Connection fillCurrentConnection() {
		try {
			Connection connection = lookupForConnection();
			// Connection is set in transactional mode because it better suits ORM usage even if it doesn't manage transactions itself
			connection.setAutoCommit(false);
			this.currentConnection.set(connection);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Connection lookupForConnection() throws SQLException {
		Connection connection;
		// Ensuring that connection is not already closed, hence trying until one is found
		try {
			connection = closedConnectionRetryer.execute(this.dataSource::getConnection, "getting a connection");
		} catch (RetryException e) {
			this.currentConnection.remove();	// just for cleanup and avoid a roundtrip on next attempt
			throw new IllegalStateException("Maximum attempt to open a connection reached : all are closed", e);
		}
		return connection;
	}
	
	/**
	 * Detaches current {@link Thread} from its {@link Connection}
	 */
	public void releaseConnection() {
		this.currentConnection.remove();
	}
	
	/**
	 * Class that asks for retry on closed {@link Connection} while opening them.
	 */
	private static class ClosedConnectionRetryer extends Retryer {
		
		public ClosedConnectionRetryer(int retryCount) {
			super(retryCount, 10);
		}
		
		@Override
		protected boolean shouldRetry(Result result) {
			if (result instanceof Failure) {
				return false;	// An exception will be thrown in case of failure while getting a connection
			}
			try {
				return ((Success<Connection>) result).getValue().isClosed();
			} catch (SQLException e) {
				throw new IllegalStateException("Impossible to know if connection is closed or not");
			}
		}
	}
}
