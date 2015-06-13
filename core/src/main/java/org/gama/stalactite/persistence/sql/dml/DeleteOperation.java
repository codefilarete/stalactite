package org.gama.stalactite.persistence.sql.dml;

import org.gama.stalactite.persistence.mapping.StatementValues;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.SQLException;
import java.util.Map;

/**
 * @author Guillaume Mary
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
	protected void applyValues(StatementValues values) throws SQLException {
		super.applyValues(values);
		applyWhereValues(whereIndexes, values);
		addBatch();
	}
}
