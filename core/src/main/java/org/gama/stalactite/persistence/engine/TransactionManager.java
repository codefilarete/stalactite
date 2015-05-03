package org.gama.stalactite.persistence.engine;

import java.sql.Connection;

/**
 * @author mary
 */
public interface TransactionManager {
	
	Connection getCurrentConnection();
	
	void executeInNewTransaction(JdbcOperation jdbcOperation);
	
	public interface JdbcOperation {
		
		public abstract void execute();
	}
}
