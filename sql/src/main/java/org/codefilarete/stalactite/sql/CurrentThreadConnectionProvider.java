package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.tool.sql.ConnectionWrapper;

/**
 * Implementation which {@link #giveConnection()} gives the current {@link Connection} as a Thread point of view. It may be detached from it
 * thanks to {@link #releaseConnection()}.
 * Connections source is the underlying {@link DataSource} given at construction time.
 * Connections are set in transactional mode through {@link Connection#setAutoCommit(boolean)} with false as parameter.
 * 
 * @author Guillaume Mary
 */
public class CurrentThreadConnectionProvider implements ConnectionProvider {
	
	private final ConnectionProvider dataSource;
	
	private final ThreadLocal<Connection> currentConnection = new ThreadLocal<>();
	
	public CurrentThreadConnectionProvider(DataSource dataSource) {
		this(new DataSourceConnectionProvider(dataSource));
	}
	
	public CurrentThreadConnectionProvider(ConnectionProvider dataSource) {
		this.dataSource = dataSource;
	}
	
	/**
	 * Implemented to provide current {@link Thread} {@link Connection}. If one doesn't exist then it is filled and will be provided on next calls.
	 * 
	 * @return current Thread {@link Connection}
	 * @see #releaseConnection()
	 * @see #fillCurrentConnection() 
	 */
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
	 * {@link Connection} is set auto-commit to false for enabling transaction mode because it better suits usual ORM usage.
	 * 
	 * @return the attached {@link Connection}
	 */
	public Connection fillCurrentConnection() {
		try {
			Connection connection = new CurrentThreadDetacherConnection(this.dataSource.giveConnection());
			// Connection is set in transactional mode because it better suits ORM usage even if it doesn't manage transactions itself
			connection.setAutoCommit(false);
			this.currentConnection.set(connection);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Detaches current {@link Thread} from its {@link Connection}
	 */
	public void releaseConnection() {
		this.currentConnection.remove();
	}
	
	/**
	 * Connection that wraps another one and will release it from current Thread on close.
	 * made to avoid that Threads keep a reference for duration between close of a connection and opening of a new one 
	 */
	private class CurrentThreadDetacherConnection extends ConnectionWrapper {
		
		private CurrentThreadDetacherConnection(Connection connection) {
			super(connection);
		}
		
		@Override
		public void close() throws SQLException {
			releaseConnection();
			super.close();
		}
	}
}
