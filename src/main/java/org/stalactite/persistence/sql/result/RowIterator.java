package org.stalactite.persistence.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.stalactite.lang.exception.Exceptions;

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
	public Row convert(ResultSet rs) {
		Row toReturn = new Row();
		try {
			for (String columnName : columnNames) {
				toReturn.put(columnName, rs.getObject(columnName));
			}
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return toReturn;
	}
}
