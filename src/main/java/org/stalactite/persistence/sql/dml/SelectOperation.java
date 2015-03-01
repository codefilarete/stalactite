package org.stalactite.persistence.sql.dml;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.stalactite.persistence.mapping.PersistentValues;
import org.stalactite.persistence.sql.result.RowIterator;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class SelectOperation extends CRUDOperation {
	
	/** Column indexes for where columns */
	private final Map<Column, Integer> whereIndexes;
	private final List<Column> selectedColumns;

	/**
	 * 
	 * @param sql le SQL de selection, attendu que ce soit un select .. from .. where ..
	 * @param whereIndexes les indices des colonnes dans le where
	 * @param selectedColumns
	 */
	public SelectOperation(String sql, Map<Column, Integer> whereIndexes, List<Column> selectedColumns) {
		super(sql);
		this.whereIndexes = whereIndexes;
		this.selectedColumns = selectedColumns;
	}
	
	public RowIterator execute() throws SQLException {
		ResultSet resultSet = getStatement().executeQuery();
		return new RowIterator(resultSet, selectedColumns);
	}
	
	@Override
	protected void applyValues(PersistentValues values) throws SQLException {
		applyWhereValues(whereIndexes, values);
	}
}
