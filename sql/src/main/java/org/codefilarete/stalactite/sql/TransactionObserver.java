package org.codefilarete.stalactite.sql;

/**
 * @author Guillaume Mary
 */
public interface TransactionObserver extends CommitObserver, RollbackObserver {
}
