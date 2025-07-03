package org.codefilarete.stalactite.sql.statement;

import java.util.Map;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;

/**
 * Equivalent to {@link PreparedSQL} but with Column as index identifier.
 * 
 * @author Guillaume Mary
 */
public class ColumnParameterizedSQL<T extends Table<T>> extends ExpandableStatement<Column<T, ?>> {
	
	private final Map<Column<T, ?>, int[]> columnIndexes;
	
	/**
	 * Detailed constructor
	 * 
	 * @param sql any SQL statement with placeholder marks ('?')
	 * @param columnIndexes mapping between {@link Column}s (used on {@link #setValue(Object, Object)} and their indexes in the SQL statement
	 * @param parameterBinders mapping between {@link Column}s and their 
	 */
	public ColumnParameterizedSQL(String sql, Map<? extends Column<T, ?>, int[]> columnIndexes, Map<? extends Column<T, ?>, ? extends PreparedStatementWriter<?>> parameterBinders) {
		super(sql, parameterBinders);
		this.columnIndexes = (Map<Column<T, ?>, int[]>) columnIndexes;
	}
	
	public ColumnParameterizedSQL(String sql, Map<? extends Column<T, ?>, int[]> columnIndexes, PreparedStatementWriterIndex<? extends Column<T, ?>, ? extends PreparedStatementWriter<?>> parameterBinderProvider) {
		super(sql, parameterBinderProvider);
		this.columnIndexes = (Map<Column<T, ?>, int[]>) columnIndexes;
	}
	
	/**
	 * Gives available columns in the statement and their positions in it as placeholder marks 
	 * @return available columns in the statement and their positions in it as placeholder marks
	 */
	public Map<Column<T, ?>, int[]> getColumnIndexes() {
		return columnIndexes;
	}
	
	@Override
	protected String getParameterName(Column<T, ?> column) {
		return column.getAbsoluteName();
	}
	
	@Override
	public int[] getIndexes(Column<T, ?> column) {
		return columnIndexes.get(column);
	}
}
