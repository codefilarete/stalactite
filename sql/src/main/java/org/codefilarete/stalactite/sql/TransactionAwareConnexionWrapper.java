package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.codefilarete.tool.sql.ConnectionWrapper;

/**
 * A {@link ConnectionWrapper} that aims at notifying {@link TransactionListener}, {@link CommitListener} or {@link RollbackListener}
 * of transaction-related methods invocations.
 * 
 * @author Guillaume Mary
 * @see CommitListener
 * @see RollbackListener
 * @see TransactionListener
 * @see TransactionListenerCollection
 */
public class TransactionAwareConnexionWrapper extends ConnectionWrapper implements TransactionObserver {
	
	private final TransactionStatusNotifier transactionStatusNotifier = new TransactionStatusNotifierSupport();
	
	public TransactionAwareConnexionWrapper() {
		this(null);
	}
	
	public TransactionAwareConnexionWrapper(Connection surrogate) {
		super(surrogate);
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
