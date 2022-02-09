package org.codefilarete.stalactite.persistence.sql.dml;

import java.util.Map;

import org.codefilarete.stalactite.sql.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.binder.PreparedStatementWriterIndex;
import org.codefilarete.stalactite.sql.dml.ExpandableStatement;
import org.codefilarete.stalactite.sql.dml.PreparedSQL;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnParameterizedSQL<T extends Table> extends ExpandableStatement<Column<T, Object>> {
	
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
	protected String getParameterName(Column column) {
		return column.getAbsoluteName();
	}
	
	/**
	 * Hides {@link org.codefilarete.stalactite.sql.dml.SQLStatement#getParameterBinder(Object)} due to class generic type erasure, but this signature allows
	 * to get the writer type that matched column's one
	 * 
	 * @param parameter any non null column
	 * @param <O> column value type
	 * @return super.getParameterBinder(parameter)
	 * @see org.codefilarete.stalactite.sql.dml.SQLStatement#getParameterBinder(Object) 
	 */
	public <O> PreparedStatementWriter<O> getParameterBinder(Column<T, O> parameter) {
		return super.getParameterBinder((Column) parameter);
	}
	
	public int[] getIndexes(Column column) {
		return columnIndexes.get(column);
	}
}
