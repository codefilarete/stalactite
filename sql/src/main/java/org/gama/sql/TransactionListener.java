package org.gama.sql;

import java.sql.Savepoint;

/**
 * @author Guillaume Mary
 */
public interface TransactionListener extends CommitListener, RollbackListener {
	
	@Override
	default void beforeCommit() {
		beforeCompletion();
	}
	
	@Override
	default void afterCommit() {
		afterCompletion();
	}
	
	@Override
	default void beforeRollback() {
		beforeCompletion();
	}
	
	@Override
	default void afterRollback() {
		afterCompletion();
	}
	
	@Override
	default void beforeRollback(Savepoint savepoint) {
		beforeCompletion(savepoint);
	}
	
	@Override
	default void afterRollback(Savepoint savepoint) {
		afterCompletion(savepoint);
	}
	
	default void afterCompletion(Savepoint savepoint) {
		afterCompletion();
	}
	
	default void beforeCompletion(Savepoint savepoint) {
		beforeCompletion();
	}
	
	void beforeCompletion();
	
	void afterCompletion();
}
