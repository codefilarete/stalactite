package org.gama.stalactite.sql;

import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Guillaume Mary
 */
public class TransactionListenerCollection implements RollbackObserver, CommitObserver, TransactionListener {
	
	private Collection<TransactionListener> transactionListeners;
	
	public TransactionListenerCollection() {
		this(new ArrayList<>());
	}
	
	public TransactionListenerCollection(Collection<TransactionListener> transactionListeners) {
		this.transactionListeners = transactionListeners;
	}
	
	@Override
	public void beforeCommit() {
		transactionListeners.forEach(TransactionListener::beforeCommit);
		beforeCompletion();
	}
	
	@Override
	public void afterCommit() {
		transactionListeners.forEach(TransactionListener::afterCommit);
		afterCompletion();
	}
	
	@Override
	public void beforeRollback() {
		transactionListeners.forEach(TransactionListener::beforeRollback);
		beforeCompletion();
	}
	
	@Override
	public void afterRollback() {
		transactionListeners.forEach(TransactionListener::afterRollback);
		afterCompletion();
	}
	
	@Override
	public void beforeRollback(Savepoint savepoint) {
		transactionListeners.forEach(l -> l.beforeRollback(savepoint));
		beforeCompletion(savepoint);
	}
	
	@Override
	public void afterRollback(Savepoint savepoint) {
		transactionListeners.forEach(l -> l.afterRollback(savepoint));
		afterCompletion(savepoint);
	}
	
	@Override
	public void beforeCompletion() {
		transactionListeners.forEach(TransactionListener::beforeCompletion);
	}
	
	@Override
	public void afterCompletion() {
		transactionListeners.forEach(TransactionListener::afterCompletion);
		transactionListeners.removeIf(TransactionListener::isTemporary);
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		transactionListeners.add(new TransactionListener() {
			
			@Override
			public void beforeCommit() {
				commitListener.beforeCommit();
			}
			
			@Override
			public void afterCommit() {
				commitListener.afterCommit();
			}
			
			@Override
			public void beforeCompletion() {
				// no completion to accomplish because the surrogate doesn't
			}
			
			@Override
			public void afterCompletion() {
				// no completion to accomplish because the surrogate doesn't
			}
			
			@Override
			public boolean isTemporary() {
				return commitListener.isTemporary();
			}
		});
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		transactionListeners.add(new TransactionListener() {
			
			@Override
			public void beforeRollback() {
				rollbackListener.beforeRollback();
			}
			
			@Override
			public void afterRollback() {
				rollbackListener.afterRollback();
			}
			
			@Override
			public void beforeRollback(Savepoint savepoint) {
				rollbackListener.beforeRollback(savepoint);
			}
			
			@Override
			public void afterRollback(Savepoint savepoint) {
				rollbackListener.afterRollback(savepoint);
			}
			
			@Override
			public void beforeCompletion() {
				// no completion to accomplish because the surrogate doesn't
			}
			
			@Override
			public void afterCompletion() {
				// no completion to accomplish because the surrogate doesn't
			}
			
			@Override
			public boolean isTemporary() {
				return rollbackListener.isTemporary();
			}
		});
	}
}
