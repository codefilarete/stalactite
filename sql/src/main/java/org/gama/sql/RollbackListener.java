package org.gama.sql;

import java.sql.Savepoint;

/**
 * @author Guillaume Mary
 */
public interface RollbackListener {
	
	void beforeRollback();
	
	void afterRollback();
	
	void beforeRollback(Savepoint savepoint);
	
	void afterRollback(Savepoint savepoint);
}
