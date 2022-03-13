package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.ConnectionProvider;

/**
 * A special {@link ConnectionProvider} that is capable of executing a SQL operation in a different (new) transaction than the one returned by
 * {@link #giveConnection()} outside of the call to {@link #executeInNewTransaction(JdbcOperation)}.
 * 
 * 
 * 
 * @author Guillaume Mary
 */
public interface SeparateTransactionExecutor extends ConnectionProvider {
	
	/**
	 * Execute the given {@link JdbcOperation} in a separate transaction: the transaction given by {@link #giveConnection()} must be different
	 * when it is called inside (by) the {@link JdbcOperation} than this given outside the call to {@link #executeInNewTransaction(JdbcOperation)}.
	 * The implementation must call {@link JdbcOperation#execute()}, so a commit should appear nearly after.
	 * 
	 * @param jdbcOperation a sql operation that will call {@link #giveConnection()} to execute its statements.
	 */
	void executeInNewTransaction(JdbcOperation jdbcOperation);
	
	/**
	 * A JDBC Operation that will ask for its {@link java.sql.Connection} to the {@link ConnectionProvider#giveConnection()}
	 */
	interface JdbcOperation {
		
		void execute();
	}
}
