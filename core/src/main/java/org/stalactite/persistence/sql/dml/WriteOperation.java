package org.stalactite.persistence.sql.dml;

import java.sql.SQLException;

/**
 * Abstract class for write operation to database. Expose batching method and updated line count after execution.
 * 
 * @author mary
 */
public abstract class WriteOperation extends CRUDOperation {
	
	/**
	 * 
	 * @param sql
	 */
	public WriteOperation(String sql) {
		super(sql);
	}
	
	public int[] execute() throws SQLException {
		return getStatement().executeBatch();
	}
	
	protected void addBatch() throws SQLException {
		getStatement().addBatch();
	}
}
