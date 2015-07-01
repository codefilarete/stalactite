package org.gama.stalactite.persistence.sql.dml;

import org.gama.sql.dml.ExpandableStatement;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.util.Map;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnPreparedSQL extends ExpandableStatement<Column> {
	
	private final Map<Column, int[]> columnIndexes;
	
	public ColumnPreparedSQL(String sql, Map<Column, int[]> columnIndexes, Map<Column, ParameterBinder> parameterBinders) {
		super(sql, parameterBinders);
		this.columnIndexes = columnIndexes;
	}
	
	@Override
	protected String getParameterName(Column column) {
		return column.getAbsoluteName();
	}
	
	public int[] getIndexes(Column column) {
		return columnIndexes.get(column);
	}
}
