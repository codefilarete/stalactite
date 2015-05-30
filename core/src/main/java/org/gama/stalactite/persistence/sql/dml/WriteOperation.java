package org.gama.stalactite.persistence.sql.dml;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.exception.MultiCauseException;
import org.gama.stalactite.persistence.mapping.StatementValues;

/**
 * Abstract class for write operation to database. Expose batching method and updated line count after execution.
 * 
 * @author mary
 */
public abstract class WriteOperation extends CRUDOperation {
	
	/** Used essentially for logging */
	private final List<StatementValues> batchedStatementValues = new ArrayList<>(100);	// should be near BatchSize
	
	/**
	 * 
	 * @param sql
	 */
	public WriteOperation(String sql) {
		super(sql);
	}
	
	public int[] execute() throws SQLException {
		int[] updatedRowCount = getStatement().executeBatch();
		checkUpdatedRowCount(updatedRowCount);
		this.batchedStatementValues.clear();
		return updatedRowCount;
	}
	
	protected void checkUpdatedRowCount(int[] updatedRowCount) {
		MultiCauseException exception = new MultiCauseException();
		for (int rowCount : updatedRowCount) {
			if (rowCount == 0) {
				exception.addCause(new IllegalStateException("Expected row update but no row updated for " + batchedStatementValues.get(rowCount).getWhereValues()));
			}
		}
		exception.throwIfNotEmpty();
	}
	
	protected void applyValues(StatementValues values) throws SQLException {
		this.batchedStatementValues.add(values);
	}
	
	protected void addBatch() throws SQLException {
		getStatement().addBatch();
	}
}
