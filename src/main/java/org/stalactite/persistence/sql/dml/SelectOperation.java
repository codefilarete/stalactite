package org.stalactite.persistence.sql.dml;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.result.RowIterator;
import org.stalactite.persistence.structure.Table.Column;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author mary
 */
public class SelectOperation extends CRUDOperation {
	
	/** Column indexes for where columns */
	private Map<Column, Integer> whereIndexes;
	
	/**
	 * 
	 * @param sql
	 * @param whereIndexes
	 */
	public SelectOperation(String sql, Map<Column, Integer> whereIndexes) {
		super(sql);
		this.whereIndexes = whereIndexes;
	}
	
	public RowIterator execute() throws SQLException {
		ResultSet resultSet = getStatement().executeQuery();
		return new RowIterator(resultSet);
	}
	
	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		applyWhereValues(whereIndexes, values);
	}
}
