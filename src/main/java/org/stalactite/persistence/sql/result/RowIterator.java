package org.stalactite.persistence.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.stalactite.lang.exception.Exceptions;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.structure.Table;

/**
 * @author mary
 */
public class RowIterator extends ResultSetIterator<Row> {
	
	private final Map<String, Table.Column> columnNames = new HashMap<>();

	/**
	 * Constructor for ResultSetIterator.
	 *
	 * @param rs a ResultSet to be wrapped in an Iterator.
	 */
	public RowIterator(ResultSet rs, Iterable<Table.Column> columnsToRead) throws SQLException {
		super(rs);
		for (Table.Column column : columnsToRead) {
			columnNames.put(column.getName(), column);
		}
	}
	
	@Override
	public Row convert(ResultSet rs) {
		Dialect currentDialect = PersistenceContext.getCurrent().getDialect();
		Row toReturn = new Row();
		try {
			for (Map.Entry<String, Table.Column> columnEntry : columnNames.entrySet()) {
				toReturn.put(columnEntry.getKey(), currentDialect.getJdbcParameterBinder().get(columnEntry.getValue(), rs));
			}
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return toReturn;
	}
}
