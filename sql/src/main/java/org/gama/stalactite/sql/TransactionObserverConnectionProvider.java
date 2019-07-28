package org.gama.stalactite.sql;

import javax.annotation.Nonnull;
import java.sql.Connection;

/**
 * A bridge between a {@link ConnectionProvider} and transaction observers as {@link CommitObserver} and {@link RollbackObserver} in order
 * to make provided {@link Connection}s observable for transactions commit and rollback.
 * 
 * @author Guillaume Mary
 */
public class TransactionObserverConnectionProvider implements ConnectionProvider, CommitObserver, RollbackObserver {
	
	private final TransactionAdapter transactionAdapter = new TransactionAdapter();
	
	private final ConnectionProvider surrogateConnectionProvider;
	
	public TransactionObserverConnectionProvider(ConnectionProvider connectionProvider) {
		this.surrogateConnectionProvider = connectionProvider;
	}
	
	@Override
	@Nonnull
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
