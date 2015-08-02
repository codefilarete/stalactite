package org.gama.stalactite.persistence.engine;

import java.sql.Connection;

/**
 * @author Guillaume Mary
 */
public interface TransactionManager {
	
	Connection getCurrentConnection();
	
	void executeInNewTransaction(JdbcOperation jdbcOperation);
	
	interface JdbcOperation {
		
		void execute();
	}
}
