package org.stalactite.persistence.sql.dml;

import java.sql.SQLException;

/**
 * @author mary
 */
public abstract class WriteOperation extends CRUDOperation<int[]> {
	
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
