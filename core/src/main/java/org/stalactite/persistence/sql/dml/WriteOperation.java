package org.stalactite.persistence.sql.dml;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.stalactite.lang.exception.MultiCauseException;
import org.stalactite.persistence.mapping.PersistentValues;

/**
 * Abstract class for write operation to database. Expose batching method and updated line count after execution.
 * 
 * @author mary
 */
public abstract class WriteOperation extends CRUDOperation {
	
	/** Used essentially for logging */
	private final List<PersistentValues> batchedPersistentValues = new ArrayList<>(100);	// should be near BatchSize
	
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
		this.batchedPersistentValues.clear();
		return updatedRowCount;
	}
	
	protected void checkUpdatedRowCount(int[] updatedRowCount) {
		MultiCauseException exception = new MultiCauseException();
		for (int rowCount : updatedRowCount) {
			if (rowCount == 0) {
				exception.addCause(new IllegalStateException("Expected row update but no row updated for " + batchedPersistentValues.get(rowCount).getWhereValues()));
			}
		}
		exception.throwIfNotEmpty();
	}
	
	protected void applyValues(PersistentValues values) throws SQLException {
		this.batchedPersistentValues.add(values);
	}
	
	protected void addBatch() throws SQLException {
		getStatement().addBatch();
	}
}
