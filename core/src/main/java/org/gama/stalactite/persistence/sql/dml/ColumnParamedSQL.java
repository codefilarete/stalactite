package org.gama.stalactite.persistence.sql.dml;

import java.util.Map;

import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.ExpandableStatement;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnParamedSQL extends ExpandableStatement<Column> {
	
	private final Map<Column, int[]> columnIndexes;
	
	public ColumnParamedSQL(String sql, Map<Column, int[]> columnIndexes, Map<Column, ParameterBinder> parameterBinders) {
		super(sql, parameterBinders);
		this.columnIndexes = columnIndexes;
	}
	
	public ColumnParamedSQL(String sql, Map<Column, int[]> columnIndexes, ParameterBinderIndex<Column> parameterBinderProvider) {
		super(sql, parameterBinderProvider);
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
