package org.stalactite.persistence.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	private final Set<String> columnNames = new HashSet<>();
	
	/**
	 * Constructor for ResultSetIterator.
	 *
	 * @param rs a ResultSet to be wrapped in an Iterator.
	 */
	public RowIterator(ResultSet rs) throws SQLException {
		super(rs);
		readColumnNames(rs);
	}
	
	protected void readColumnNames(ResultSet rs) throws SQLException {
		int resultSetColumnCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= resultSetColumnCount; i++) {
			columnNames.add(rs.getMetaData().getColumnName(i));
		}
	}
	
	@Override
	protected Row convert(ResultSet rs) throws SQLException {
		Row toReturn = new Row();
		for (String columnName : columnNames) {
			toReturn.put(columnName, rs.getObject(columnName));
		}
		return toReturn;
	}
}
