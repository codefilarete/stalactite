package org.gama.sql;

/**
 * @author Guillaume Mary
 */
public interface CommitObserver {
	
	void addCommitListener(CommitListener commitListener);
}
