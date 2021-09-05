package org.gama.stalactite.sql;

import javax.annotation.Nonnull;
import java.sql.Connection;

/**
 * A bridge between a {@link ConnectionProvider} and transaction observers ({@link CommitObserver}, {@link RollbackObserver}) in order
 * to make provided {@link Connection}s observable for transaction commit and rollback.
 * 
 * @author Guillaume Mary
 */
public class TransactionAwareConnectionProvider implements ConnectionProvider, TransactionObserver {
	
	private final TransactionAwareConnexionWrapper transactionAwareConnexionWrapper = new TransactionAwareConnexionWrapper();
	
	private final ConnectionProvider surrogate;
	
	public TransactionAwareConnectionProvider(ConnectionProvider connectionProvider) {
		this.surrogate = connectionProvider;
	}
	
	public ConnectionProvider getSurrogate() {
		return surrogate;
	}
	
	@Override
	@Nonnull
	public Connection giveConnection() {
		Connection connection = surrogate.giveConnection();
		transactionAwareConnexionWrapper.setSurrogate(connection);
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
