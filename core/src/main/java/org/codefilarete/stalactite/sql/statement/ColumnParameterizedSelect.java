package org.codefilarete.stalactite.sql.statement;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;
import org.codefilarete.tool.collection.Iterables;

/**
 * Specialization of {@link ColumnParameterizedSQL} for a select statement: gives access to selected columns through {@link #getSelectParameterBinders()}
 * 
 * @author Guillaume Mary
 */
public class ColumnParameterizedSelect<T extends Table<T>> extends ColumnParameterizedSQL<T> {
	
	private final ParameterBinderIndex<Selectable<?>, ParameterBinder<?>> selectParameterBinders;
	private final Map<Selectable<?>, String> aliases;
	
	public ColumnParameterizedSelect(String sql,
									 Map<? extends Column<T, ?>, int[]> columnIndexes,
									 Map<? extends Column<T, ?>, ? extends ParameterBinder<?>> parameterBinders,
									 Map<? extends Selectable<?>, ? extends ParameterBinder<?>> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinders);
		this.selectParameterBinders = (ParameterBinderIndex<Selectable<?>, ParameterBinder<?>>) ParameterBinderIndex.fromMap(selectParameterBinders);
		this.aliases = Iterables.map(selectParameterBinders.keySet(), Function.identity(), Selectable::getExpression);
	}

	public ColumnParameterizedSelect(String sql,
									 Map<? extends Column<T, ?>, int[]> columnIndexes,
									 Map<? extends Column<T, ?>, ? extends ParameterBinder<?>> parameterBinders,
									 Map<? extends Selectable<?>, ? extends ParameterBinder<?>> selectParameterBinders,
									 Map<Selectable<?>, String> aliases) {
		super(sql, columnIndexes, parameterBinders);
		this.selectParameterBinders = (ParameterBinderIndex<Selectable<?>, ParameterBinder<?>>) ParameterBinderIndex.fromMap(selectParameterBinders);
		this.aliases = aliases;
	}
	
	public ColumnParameterizedSelect(String sql,
									 Map<? extends Column<T, ?>, int[]> columnIndexes,
									 PreparedStatementWriterIndex<? extends Column<T, ?>, ? extends ParameterBinder<?>> parameterBinderProvider,
									 ParameterBinderIndex<? extends Selectable<?>, ParameterBinder<?>> selectParameterBinders) {
		super(sql, columnIndexes, parameterBinderProvider);
		this.selectParameterBinders = (ParameterBinderIndex<Selectable<?>, ParameterBinder<?>>) selectParameterBinders;
		this.aliases = Iterables.map(selectParameterBinders.keys(), Function.identity(), Selectable::getExpression);
	}
	
	public ParameterBinderIndex<Selectable<?>, ParameterBinder<?>> getSelectParameterBinders() {
		return selectParameterBinders;
	}
	
	public Map<Selectable<?>, String> getAliases() {
		return aliases;
	}
}
