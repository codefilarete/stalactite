package org.codefilarete.stalactite.sql.statement;

import java.util.Map;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnParameterizedSQL<T extends Table<T>> extends ExpandableStatement<Column<T, Object>> {
	
	private final Map<Column<T, Object>, int[]> columnIndexes;
	
	/**
	 * Detailed constructor
	 * 
	 * @param sql any SQL statement with placeholder marks ('?')
	 * @param columnIndexes mapping between {@link Column}s (used on {@link #setValue(Object, Object)} and their indexes in the SQL statement
	 * @param parameterBinders mapping between {@link Column}s and their 
	 */
	public ColumnParameterizedSQL(String sql, Map<Column<T, Object>, int[]> columnIndexes, Map<Column<T, Object>, ? extends PreparedStatementWriter> parameterBinders) {
		super(sql, parameterBinders);
		this.columnIndexes = columnIndexes;
	}
	
	public ColumnParameterizedSQL(String sql, Map<Column<T, Object>, int[]> columnIndexes, PreparedStatementWriterIndex<Column<T, Object>, ParameterBinder> parameterBinderProvider) {
		super(sql, parameterBinderProvider);
		this.columnIndexes = columnIndexes;
	}
	
	@Override
	protected String getParameterName(Column<T, Object> column) {
		return column.getAbsoluteName();
	}
	
	@Override
	public int[] getIndexes(Column<T, Object> column) {
		return columnIndexes.get(column);
	}
}
