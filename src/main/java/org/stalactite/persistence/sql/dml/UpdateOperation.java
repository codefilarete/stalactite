package org.stalactite.persistence.sql.dml;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

import java.sql.SQLException;
import java.util.Map;

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
	
	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		applyUpsertValues(updateIndexes, values);
		applyWhereValues(whereIndexes, values);
		addBatch();
	}
}
