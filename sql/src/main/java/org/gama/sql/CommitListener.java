package org.gama.sql;

/**
 * Default contract for listening to transaction commit
 * 
 * @author Guillaume Mary
 */
public interface CommitListener {
	
	void beforeCommit();
	
	void afterCommit();
	
	/**
	 * Tells if this listener must be removed after transaction completion
	 * @return false
	 */
	default boolean isTemporary() {
		return false;
	}
}
