package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Set;

import org.codefilarete.tool.sql.ConnectionWrapper;

/**
 * A {@link ConnectionWrapper} that aims at notifying some {@link TransactionListener}s ({@link CommitListener} or {@link RollbackListener})
 * of transaction-related methods invocations of the underlying {@link Connection}.
 * 
 * @author Guillaume Mary
 * @see CommitListener
 * @see RollbackListener
 * @see TransactionListener
 * @see TransactionListenerCollection
 */
public class TransactionAwareConnexionWrapper extends ConnectionWrapper implements TransactionObserver {
	
	private final TransactionStatusNotifier transactionStatusNotifier = new TransactionStatusNotifierSupport();
	
	/**
	 * Default constructor without a delegate {@link Connection}.
	 * Though, caller must invoke #setDelegate(Connection) to set a real connection.
	 */
	public TransactionAwareConnexionWrapper() {
		this(null);
	}
	
	/**
	 * Default constructor with a delegate {@link Connection}.
	 * @param delegate the delegate {@link Connection}
	 */
	public TransactionAwareConnexionWrapper(Connection delegate) {
		super(delegate);
	}
	
	/**
	 * Constructor with a set of listeners but without a delegate {@link Connection}
	 * @param commitListeners a set of {@link CommitListener}
	 * @param rollbackListeners a set of {@link RollbackListener}
	 */
	public TransactionAwareConnexionWrapper(Set<CommitListener> commitListeners, Set<RollbackListener> rollbackListeners) {
		commitListeners.forEach(this::addCommitListener);
		rollbackListeners.forEach(this::addRollbackListener);
	}
	
	/**
	 * Complete constructor with a delegate {@link Connection} and a set of listeners.
	 * @param delegate the delegate {@link Connection}
	 * @param commitListeners a set of {@link CommitListener}
	 * @param rollbackListeners a set of {@link RollbackListener}
	 */
	public TransactionAwareConnexionWrapper(Connection delegate, Set<CommitListener> commitListeners, Set<RollbackListener> rollbackListeners) {
		this(delegate);
		commitListeners.forEach(this::addCommitListener);
		rollbackListeners.forEach(this::addRollbackListener);
	}
	
	public TransactionStatusNotifier getTransactionStatusNotifier() {
		return transactionStatusNotifier;
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		transactionStatusNotifier.addCommitListener(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		transactionStatusNotifier.addRollbackListener(rollbackListener);
	}
	
	@Override
	public void commit() throws SQLException {
		this.transactionStatusNotifier.transactionCommitTriggered();
		super.commit();
		this.transactionStatusNotifier.transactionCommitted();
	}
	
	@Override
	public void rollback() throws SQLException {
		this.transactionStatusNotifier.transactionRollbackTriggered();
		super.rollback();
		this.transactionStatusNotifier.transactionRollbacked();
	}
	
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		this.transactionStatusNotifier.transactionRollbackTriggered(savepoint);
		super.rollback(savepoint);
		this.transactionStatusNotifier.transactionRollbacked(savepoint);
	}
}
