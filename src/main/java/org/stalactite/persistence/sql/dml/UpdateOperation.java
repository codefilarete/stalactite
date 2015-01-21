package org.stalactite.persistence.sql.dml;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class UpdateOperation extends WriteOperation {
	
	/** Column indexes for written columns */
	private Map<Column, Integer> updateIndexes;
	
	/** Column indexes for where columns */
	private Map<Column, Integer> whereIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param updateIndexes
	 */
	public UpdateOperation(String sql, Map<Column, Integer> updateIndexes, Map<Column, Integer> whereIndexes) {
		super(sql);
		this.updateIndexes = updateIndexes;
		this.whereIndexes = whereIndexes;
	}
	
	public void setUpdateValue(@Nonnull Column column, Object value) throws SQLException {
		set(updateIndexes, column, value);
	}
	
	public void setWhereValue(@Nonnull Column column, Object value) throws SQLException {
		set(whereIndexes, column, value);
	}
	
	protected void applyValues(PersistentValues values) throws SQLException {
		for (Entry<Column, Object> colToValues : values.getUpsertValues().entrySet()) {
			setUpdateValue(colToValues.getKey(), colToValues.getValue());
		}
		for (Entry<Column, Object> colToValues : values.getWhereValues().entrySet()) {
			setWhereValue(colToValues.getKey(), colToValues.getValue());
		}
		addBatch();
	}
}
