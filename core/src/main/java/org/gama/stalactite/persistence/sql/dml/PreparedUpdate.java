package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.PreparedSQL;
import org.gama.sql.dml.SQLStatement;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class PreparedUpdate extends SQLStatement<UpwhereColumn> {
	
	private final String sql;
	private final Map<UpwhereColumn, Integer> columnIndexes;
	
	public PreparedUpdate(String sql, Map<UpwhereColumn, Integer> columnIndexes, Map<UpwhereColumn, ParameterBinder> parameterBinders) {
		super(parameterBinders);
		this.sql = sql;
		this.columnIndexes = columnIndexes;
	}
	
	@Override
	public String getSQL() {
		return sql;
	}
	
	@Override
	protected void doApplyValue(UpwhereColumn column, Object value, PreparedStatement statement) {
		ParameterBinder parameterBinder = getParameterBinder(column);
		if (parameterBinder == null) {
			throw new IllegalArgumentException("Can't find a "+ParameterBinder.class.getName() + " for column " + column.column.getAbsoluteName() + " of value " + value
					+ " on sql : " + getSQL());
		}
		doApplyValue(getIndex(column), value, parameterBinder, statement);
	}
	
	private void doApplyValue(int index, Object value, ParameterBinder parameterBinder, PreparedStatement statement) {
		try {
			parameterBinder.set(index, value, statement);
		} catch (SQLException e) {
			throw new RuntimeException("Error while setting value " + value + " for parameter " + index + " on statement " + getSQL(), e);
		}
	}
	
	public int getIndex(UpwhereColumn column) {
		return columnIndexes.get(column);
	}
	
	public static class UpwhereColumn {
		
		private final Column column;
		private final boolean update;
		
		public UpwhereColumn(Column column, boolean update) {
			this.column = column;
			this.update = update;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UpwhereColumn) {
				UpwhereColumn other = (UpwhereColumn) obj;
				return this.column.equals(other.column) && this.update == other.update;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return this.column.hashCode();
		}
		
		@Override
		public String toString() {
			return column.getAbsoluteName() + (update ? " (U)" : " (W)");
		}
	}
}
