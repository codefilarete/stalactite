package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.SQLStatement;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.sql.PreparedStatement;
import java.util.*;
import java.util.Map.Entry;

/**
 * Statement dedicated to updates: a parameter can be in the where clause and the update one with different values.
 * So we must distinct those parameters. This is done with {@link UpwhereColumn}.
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
	
	public int getIndex(UpwhereColumn column) {
		return columnIndexes.get(column);
	}
	
	/**
	 * Wrapper for Column placed in an update statement so it can distinguish if it's for the Update or Where part 
	 */
	public static class UpwhereColumn {
		
		public static Set<Column> getUpdateColumns(Set<UpwhereColumn> set) {
			Set<Column> updateColumns = new HashSet<>();
			for (UpwhereColumn upwhereColumn : set) {
				if (upwhereColumn.update) {
					updateColumns.add(upwhereColumn.column);
				}
			}
			return updateColumns;
		}
		
		public static Map<Column, Object> getUpdateColumns(Map<UpwhereColumn, Object> map) {
			Map<Column, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn, Object> entry : map.entrySet()) {
				UpwhereColumn upwhereColumn = entry.getKey();
				if (upwhereColumn.update) {
					updateColumns.put(upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
		public static Map<Column, Object> getWhereColumns(Map<UpwhereColumn, Object> map) {
			Map<Column, Object> updateColumns = new HashMap<>();
			for (Entry<UpwhereColumn, Object> entry : map.entrySet()) {
				UpwhereColumn upwhereColumn = entry.getKey();
				if (!upwhereColumn.update) {
					updateColumns.put(upwhereColumn.column, entry.getValue());
				}
			}
			return updateColumns;
		}
		
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
