package org.stalactite.persistence.sql.dml;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

import java.sql.SQLException;
import java.util.Map;

/**
 * @author mary
 */
public class DeleteOperation extends WriteOperation {
	
	/** Column indexes for where columns */
	private Map<Column, Integer> whereIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param whereIndexes
	 */
	public DeleteOperation(String sql, Map<Column, Integer> whereIndexes) {
		super(sql);
		this.whereIndexes = whereIndexes;
	}
	
	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		applyWhereValues(whereIndexes, values);
		addBatch();
	}
}
