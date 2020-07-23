package org.gama.stalactite.sql;

import java.sql.Savepoint;

/**
 * @author Guillaume Mary
 */
public class TransactionStatusNotifierSupport implements TransactionStatusNotifier, TransactionObserver {
	
	private final TransactionListenerCollection transactionListenerCollection = new TransactionListenerCollection();
	
	public TransactionStatusNotifierSupport() {
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		transactionListenerCollection.addCommitListener(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		transactionListenerCollection.addRollbackListener(rollbackListener);
	}
	
	@Override
	public void transactionCommitTriggered() {
		transactionListenerCollection.beforeCommit();
	}
	
	@Override
	public void transactionCommitted() {
		transactionListenerCollection.afterCommit();
	}
	
	@Override
	public void transactionRollbackTriggered() {
		transactionListenerCollection.beforeRollback();
	}
	
	@Override
	public void transactionRollbacked() {
		transactionListenerCollection.afterRollback();
	}
	
	@Override
	public void transactionRollbackTriggered(Savepoint savepoint) {
		transactionListenerCollection.beforeRollback(savepoint);
	}
	
	@Override
	public void transactionRollbacked(Savepoint savepoint) {
		transactionListenerCollection.afterRollback(savepoint);
	}
}
