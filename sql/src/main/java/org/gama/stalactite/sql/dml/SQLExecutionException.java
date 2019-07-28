package org.gama.stalactite.sql.dml;

/**
 * Dedicated class to execption happening on SQL execution
 * 
 * @author Guillaume Mary
 */
public class SQLExecutionException extends RuntimeException {
	
	public SQLExecutionException() {
	}
	
	public SQLExecutionException(String sql) {
		super(sql);
	}
	
	public SQLExecutionException(String sql, Throwable cause) {
		super(sql, cause);
	}
	
	public SQLExecutionException(Throwable cause) {
		super(cause);
	}
	
	@Override
	public String getMessage() {
		return "Error while executing \"" + super.getMessage() + "\"";
	}
}
