package org.codefilarete.stalactite.sql;

import java.sql.Savepoint;

/**
 * Default contract for listening to transaction rollback
 * 
 * @author Guillaume Mary
 */
public interface RollbackListener {
	
	void beforeRollback();
	
	void afterRollback();
	
	void beforeRollback(Savepoint savepoint);
	
	void afterRollback(Savepoint savepoint);
	
	/**
	 * Tells if this listener must be removed after transaction completion
	 * @return false
	 */
	default boolean isTemporary() {
		return false;
	}
}
