package org.codefilarete.stalactite.sql.statement;

import java.util.Map;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;

/**
 * Specialization of {@link ColumnParameterizedSQL} for select statement: gives access to selected columns through {@link #getSelectParameterBinders()}
 * 
 * @author Guillaume Mary
 */
public class ColumnParameterizedSelect<T extends Table<T>> extends ColumnParameterizedSQL<T> {
	
	private final ParameterBinderIndex<String, ParameterBinder<?>> selectParameterBinders;
	
	public ColumnParameterizedSelect(String sql,
									 Map<Column<T, ?>, int[]> columnIndexes,
									 Map<Column<T, ?>, ? extends ParameterBinder<?>> parameterBinders,
									 Map<String, ? extends ParameterBinder<?>> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinders);
		this.selectParameterBinders = (ParameterBinderIndex<String, ParameterBinder<?>>) ParameterBinderIndex.fromMap(selectParameterBinders);
	}
	
	public ColumnParameterizedSelect(String sql,
									 Map<Column<T, ?>, int[]> columnIndexes,
									 PreparedStatementWriterIndex<Column<T, ?>, ? extends ParameterBinder<?>> parameterBinderProvider,
									 Map<String, ParameterBinder<?>> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinderProvider);
		this.selectParameterBinders = ParameterBinderIndex.fromMap(selectParameterBinders);
	}
	
	public ColumnParameterizedSelect(String sql,
									 Map<Column<T, ?>, int[]> columnIndexes,
									 PreparedStatementWriterIndex<Column<T, ?>, ? extends ParameterBinder<?>> parameterBinderProvider,
									 ParameterBinderIndex<String, ParameterBinder<?>> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinderProvider);
		this.selectParameterBinders = selectParameterBinders;
	}
	
	public ParameterBinderIndex<String, ParameterBinder<?>> getSelectParameterBinders() {
		return selectParameterBinders;
	}
}
