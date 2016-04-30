package org.gama.stalactite.test;

import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.engine.TransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Very simple TransactionManager for JDBC connection.
 * Not for production use. Mono-threaded.
 * 
 * @author Guillaume Mary
 */
public class JdbcTransactionManager implements TransactionManager {
	
	private DataSource dataSource;
	/** LIFO stack of used connection. Don't support multi-thread access */
	private Deque<Connection> currentConnection = new ArrayDeque<>();
	
	public JdbcTransactionManager(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public DataSource getDataSource() {
		return dataSource;
	}
	
	public void setDataSource(DataSource dataSource) {
		clearConnections();	// on enlève les anciennes connexions puisqu'on change de datasource
		this.dataSource = dataSource;
	}
	
	@Override
	public Connection getCurrentConnection() {
		Connection connectionOnTop = this.currentConnection.peek();
		try {
			if (connectionOnTop == null || connectionOnTop.isClosed()) {
				this.currentConnection.poll();	// only in case of closed connection
				offerNewConnection();
				connectionOnTop = this.currentConnection.peek();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return connectionOnTop;
	}
	
	protected void offerNewConnection() {
		try {
			Connection connection = this.dataSource.getConnection();
			connection.setAutoCommit(false);
			this.currentConnection.offerFirst(connection);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		offerNewConnection();	// force new connection in queue
		Connection connection = null;
		try {
			connection = getCurrentConnection();
			jdbcOperation.execute();
			connection.commit();
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
					throw Exceptions.asRuntimeException(e);
				} catch (SQLException e1) {
					throw Exceptions.asRuntimeException(e1);
				}
			}
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				currentConnection.poll();
			}
		}
	}
	
	private void clearConnections() {
		for (Connection connection : currentConnection) {
			try {
				connection.close();
			} catch (SQLException e) {
				// slient close
			}
		}
		currentConnection.clear();
	}
}
