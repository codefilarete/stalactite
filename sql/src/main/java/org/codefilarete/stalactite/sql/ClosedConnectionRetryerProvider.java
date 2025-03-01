package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import org.codefilarete.tool.Retryer;
import org.codefilarete.tool.Retryer.RetryException;

/**
 * {@link ConnectionProvider} that adds some retry attempts when getting a connection from another {@link ConnectionProvider}.
 * Retries check that the connection is not closed. A delay is observed between retries.
 * 
 * @author Guillaume Mary
 */
public class ClosedConnectionRetryerProvider implements ConnectionProvider {
	
	private final ConnectionProvider dataSource;
	
	private final ClosedConnectionRetryer closedConnectionRetryer;
	
	/**
	 * Default constructor that will attempt 3 times, with a delay of 10ms between each, before throwing an exception on connection providence.
	 * Given {@link ConnectionProvider} is expected to be a simple one such as {@link ConnectionProvider.DataSourceConnectionProvider}
	 * or {@link SimpleConnectionProvider}.
	 * 
	 * @param dataSource the underlying {@link ConnectionProvider}
	 */
	public ClosedConnectionRetryerProvider(ConnectionProvider dataSource) {
		this(dataSource, 3);
	}
	
	public ClosedConnectionRetryerProvider(ConnectionProvider dataSource, int connectionOpeningRetryMaxCount) {
		this.dataSource = dataSource;
		this.closedConnectionRetryer = new ClosedConnectionRetryer(connectionOpeningRetryMaxCount, Duration.ofMillis(10));
	}
	
	public ClosedConnectionRetryerProvider(ConnectionProvider dataSource, int connectionOpeningRetryMaxCount, Duration connectionOpeningRetryInterval) {
		this.dataSource = dataSource;
		this.closedConnectionRetryer = new ClosedConnectionRetryer(connectionOpeningRetryMaxCount, connectionOpeningRetryInterval);
	}
	
	@Override
	public Connection giveConnection() {
		return lookupForConnection();
	}
	
	private Connection lookupForConnection() {
		Connection connection;
		// Ensuring that connection is not already closed, hence trying until one is found
		try {
			connection = closedConnectionRetryer.execute(this.dataSource::giveConnection, "getting a connection");
		} catch (RetryException e) {
			throw new RetryException("Maximum attempt to open a connection reached : all are closed", e);
		}
		return connection;
	}
	
	
	/**
	 * Class that asks for retry on closed {@link Connection} when obtaining them.
	 */
	private static class ClosedConnectionRetryer extends Retryer {
		
		public ClosedConnectionRetryer(int retryCount, Duration connectionOpeningRetryInterval) {
			super(retryCount, connectionOpeningRetryInterval);
		}
		
		@Override
		protected boolean shouldRetry(Result result) {
			if (result instanceof Failure) {
				return false;	// An exception will be thrown in case of failure while getting a connection
			}
			try {
				return ((Success<Connection>) result).getValue().isClosed();
			} catch (SQLException e) {
				throw new IllegalStateException("Impossible to know if connection is closed or not", e);
			}
		}
	}
}
