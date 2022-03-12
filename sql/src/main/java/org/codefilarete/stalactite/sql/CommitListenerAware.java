package org.codefilarete.stalactite.sql;

/**
 * @author Guillaume Mary
 */
public interface CommitListenerAware {
	
	void addCommitListener(CommitListener commitListener);
}
