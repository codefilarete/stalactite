package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Set;

import org.codefilarete.stalactite.sql.CommitListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.TransactionAwareConnexionWrapper;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.exception.Exceptions;

/**
 * {@link org.codefilarete.stalactite.sql.ConnectionProvider} made to match expected contract of {@link PersistenceContext} connection configuration.
 * This implementation uses {@link ThreadLocal} to store and provide connections.
 * 
 * @author Guillaume Mary
 */
public class CurrentThreadTransactionalConnectionProvider implements ConnectionConfiguration.TransactionalConnectionProvider {
	
	private final CurrentThreadConnectionProvider jdbcConnectionProvider;
	
	private final Set<CommitListener> commitListeners = new KeepOrderSet<>();
	private final Set<RollbackListener> rollbackListeners = new KeepOrderSet<>();
	
	public CurrentThreadTransactionalConnectionProvider(DataSource dataSource) {
		this.jdbcConnectionProvider = new CurrentThreadConnectionProvider(dataSource);
	}
	
	public CurrentThreadTransactionalConnectionProvider(DataSource dataSource, int connectionOpeningRetryMaxCount) {
		this.jdbcConnectionProvider = new CurrentThreadConnectionProvider(dataSource, connectionOpeningRetryMaxCount);
	}
	
	@Override
	public Connection giveConnection() {
		return new TransactionAwareConnexionWrapper(this.jdbcConnectionProvider.giveConnection(), commitListeners, rollbackListeners);
	}
	
	/**
	 * Implementation based on a {@link Savepoint} on current {@link Connection} to execute given operation.
	 * 
	 * @param jdbcOperation a sql operation that will call {@link #giveConnection()} to execute its statements.
	 */
	@Override
	@SuppressWarnings("resource" /* put for non-closed Connection which is normal here */)
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		Connection currentConnection = giveConnection();
		Savepoint savepoint = null;
		try {
			savepoint = currentConnection.setSavepoint();
			jdbcOperation.execute(currentConnection);
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
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		this.commitListeners.add(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		this.rollbackListeners.add(rollbackListener);
	}
}
