package org.gama.sql;

import java.sql.Connection;

/**
 * @author Guillaume Mary
 */
public class TransactionObserverConnectionProvider implements ConnectionProvider, CommitObserver, RollbackObserver {
	
	private final TransactionAdapter transactionAdapter = new TransactionAdapter();
	
	private final ConnectionProvider surrogateConnectionProvider;
	
	public TransactionObserverConnectionProvider(ConnectionProvider connectionProvider) {
		this.surrogateConnectionProvider = connectionProvider;
	}
	
	@Override
	public Connection getCurrentConnection() {
		Connection connection = surrogateConnectionProvider.getCurrentConnection();
		transactionAdapter.setSurrogate(connection);
		return transactionAdapter;
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		this.transactionAdapter.addCommitListener(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		this.transactionAdapter.addRollbackListener(rollbackListener);
	}
}
