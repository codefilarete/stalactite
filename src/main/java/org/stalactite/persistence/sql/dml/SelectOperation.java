package org.stalactite.persistence.sql.dml;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class SelectOperation extends CRUDOperation<ResultSet> {
	
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
	
	public void setValue(@Nonnull Column column, Object value) throws SQLException {
		set(whereIndexes, column, value);
	}
	
	public ResultSet execute() throws SQLException {
		return getStatement().executeQuery();
	}
	
	protected void applyValues(PersistentValues values) throws SQLException {
		for (Entry<Column, Object> colToValues : values.getWhereValues().entrySet()) {
			setValue(colToValues.getKey(), colToValues.getValue());
		}
	}
}
