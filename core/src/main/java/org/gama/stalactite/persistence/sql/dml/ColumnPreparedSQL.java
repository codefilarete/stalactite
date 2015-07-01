package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.SQLStatement;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnPreparedSQL extends SQLStatement<Column> {
	
	private final String sql;
	private final Map<Column, int[]> columnIndexes;
	
	public ColumnPreparedSQL(String sql, Map<Column, int[]> columnIndexes, Map<Column, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
		this.columnIndexes = columnIndexes;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(Column column, Object value, PreparedStatement statement) {
		ParameterBinder parameterBinder = getParameterBinder(column);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for column " + column.getAbsoluteName() + " of value " + value
					+ " on sql : " + getSQL());
		}
		int[] markIndexes = getIndexes(column);
		if (markIndexes.length == 1 && !(value instanceof Iterable)) {
			int index = markIndexes[0];
			doApplyValue(index, value, parameterBinder, statement);
		} else {
			int firstIndex = markIndexes[0];
			for (Object v : (Iterable) value) {
				doApplyValue(firstIndex++, v, parameterBinder, statement);
			}
		}
	}
	
	private void doApplyValue(int index, Object value, ParameterBinder parameterBinder, PreparedStatement statement) {
		try {
			parameterBinder.set(index, value, statement);
		} catch (SQLException e) {
			throw new RuntimeException("Error while setting value " + value + " for parameter " + index + " on statement " + getSQL(), e);
		}
	}
	
	public int[] getIndexes(Column column) {
		return columnIndexes.get(column);
	}
}
