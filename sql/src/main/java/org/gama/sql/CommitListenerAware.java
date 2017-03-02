package org.gama.sql;

/**
 * @author Guillaume Mary
 */
public interface CommitListenerAware {
	
	void addCommitListener(CommitListener commitListener);
}
