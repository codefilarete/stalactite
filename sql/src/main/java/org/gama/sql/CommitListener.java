package org.gama.sql;

/**
 * @author Guillaume Mary
 */
public interface CommitListener {
	
	void beforeCommit();
	
	void afterCommit();
	
}
