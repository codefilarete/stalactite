package org.gama.stalactite.sql;

/**
 * @author Guillaume Mary
 */
public interface TransactionObserver extends CommitObserver, RollbackObserver {
}
