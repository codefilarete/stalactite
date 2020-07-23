package org.gama.stalactite.sql;

import java.sql.Savepoint;

/**
 * @author Guillaume Mary
 */
public interface TransactionStatusNotifier extends TransactionObserver {
	
	void addCommitListener(CommitListener commitListener);
	
	void addRollbackListener(RollbackListener rollbackListener);
	
	void transactionCommitTriggered();
	
	void transactionRollbackTriggered();
	
	void transactionCommitted();
	
	void transactionRollbacked();
	
	void transactionRollbackTriggered(Savepoint savepoint);
	
	void transactionRollbacked(Savepoint savepoint);
}
