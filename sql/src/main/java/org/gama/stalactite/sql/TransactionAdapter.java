package org.gama.stalactite.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import org.gama.lang.sql.ConnectionWrapper;

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
public class TransactionAdapter extends ConnectionWrapper implements CommitObserver, RollbackObserver {
	
	private final TransactionListenerCollection transactionListenerCollection;
	
	public TransactionAdapter() {
		this(null);
	}
	
	public TransactionAdapter(Connection surrogate) {
		this(surrogate, new TransactionListenerCollection());
	}
	
	public TransactionAdapter(Connection surrogate, TransactionListenerCollection transactionListenerCollection) {
		super(surrogate);
		this.transactionListenerCollection = transactionListenerCollection;
	}
	
	@Override
	public void commit() throws SQLException {
		transactionListenerCollection.beforeCommit();
		super.commit();
		transactionListenerCollection.afterCommit();
	}
	
	@Override
	public void rollback() throws SQLException {
		transactionListenerCollection.beforeRollback();
		super.rollback();
		transactionListenerCollection.afterRollback();
	}
	
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		transactionListenerCollection.beforeRollback(savepoint);
		super.rollback(savepoint);
		transactionListenerCollection.afterRollback(savepoint);
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		transactionListenerCollection.addCommitListener(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		transactionListenerCollection.addRollbackListener(rollbackListener);
	}
}
