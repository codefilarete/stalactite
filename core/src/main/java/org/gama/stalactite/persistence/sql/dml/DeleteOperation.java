package org.gama.stalactite.persistence.sql.dml;

import java.sql.SQLException;
import java.util.Map;

import org.gama.stalactite.persistence.mapping.PersistentValues;
import org.gama.stalactite.persistence.sql.dml.binder.ParameterBinder;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class DeleteOperation extends WriteOperation {
	
	/** Column indexes for where columns */
	private Map<Column, Map.Entry<Integer, ParameterBinder>> whereIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param whereIndexes
	 */
	public DeleteOperation(String sql, Map<Column, Integer> whereIndexes) {
		super(sql);
		this.whereIndexes = getBinders(whereIndexes);
	}

	public Map<Column, Map.Entry<Integer, ParameterBinder>> getWhereIndexes() {
		return whereIndexes;
	}

	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		super.applyValues(values);
		applyWhereValues(whereIndexes, values);
		addBatch();
	}
}
