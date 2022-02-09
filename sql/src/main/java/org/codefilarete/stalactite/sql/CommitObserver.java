package org.codefilarete.stalactite.sql;

/**
 * @author Guillaume Mary
 */
public interface CommitObserver {
	
	void addCommitListener(CommitListener commitListener);
}
