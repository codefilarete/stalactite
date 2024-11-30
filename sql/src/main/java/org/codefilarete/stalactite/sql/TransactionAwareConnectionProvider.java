package org.codefilarete.stalactite.sql;

import java.sql.Connection;

/**
 * A bridge between a {@link ConnectionProvider} and transaction observers ({@link CommitObserver}, {@link RollbackObserver}) in order
 * to make provided {@link Connection}s observable for transaction commit and rollback.
 * 
 * @author Guillaume Mary
 */
public class TransactionAwareConnectionProvider implements ConnectionProvider, TransactionObserver {
	
	private final TransactionAwareConnexionWrapper transactionAwareConnexionWrapper = new TransactionAwareConnexionWrapper();
	
	private final ConnectionProvider delegate;
	
	public TransactionAwareConnectionProvider(ConnectionProvider connectionProvider) {
		this.delegate = connectionProvider;
	}
	
	public ConnectionProvider getDelegate() {
		return delegate;
	}
	
	@Override
	public Connection giveConnection() {
		Connection connection = delegate.giveConnection();
		transactionAwareConnexionWrapper.setDelegate(connection);
		return transactionAwareConnexionWrapper;
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		this.transactionAwareConnexionWrapper.addCommitListener(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		this.transactionAwareConnexionWrapper.addRollbackListener(rollbackListener);
	}
}
