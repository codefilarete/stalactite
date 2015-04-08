package org.stalactite.persistence.sql.dml;

import java.sql.SQLException;
import java.util.Map;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.dml.binder.ParameterBinder;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class UpdateOperation extends WriteOperation {
	
	/** Column indexes for written columns */
	private Map<Column, Map.Entry<Integer, ParameterBinder>> updateIndexes;
	
	/** Column indexes for where columns */
	private Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param updateIndexes
	 */
	public UpdateOperation(String sql, Map<Column, Integer> updateIndexes, Map<Column, Integer> whereIndexes) {
		super(sql);
		this.updateIndexes = getBinders(updateIndexes);
		this.whereIndexes = getBinders(whereIndexes);
	}

	public Map<Column,Map.Entry<Integer,ParameterBinder>> getUpdateIndexes() {
		return updateIndexes;
	}

	public Map<Column,Map.Entry<Integer,ParameterBinder>> getWhereIndexes() {
		return whereIndexes;
	}

	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		super.applyValues(values);
		applyUpsertValues(updateIndexes, values);
		applyWhereValues(whereIndexes, values);
		addBatch();
	}
}
