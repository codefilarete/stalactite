package org.codefilarete.stalactite.sql.order;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.codefilarete.stalactite.query.builder.ExpandableSQLAppender;
import org.codefilarete.stalactite.query.builder.PreparableSQLBuilder;
import org.codefilarete.stalactite.query.builder.PreparedSQLAppender;
import org.codefilarete.stalactite.query.builder.SQLAppender;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.builder.StringSQLAppender;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.Placeholder;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.MutableInt;

/**
 * A SQL builder for {@link Delete} objects
 * Can hardly be shared with {@link DMLGenerator} because the latter doesn't handle multi
 * tables update.
 * 
 * @author Guillaume Mary
 */
public class DeleteCommandBuilder<T extends Table<T>> implements SQLBuilder, PreparableSQLBuilder {
	
	private final Delete<T> delete;
	private final Dialect dialect;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public DeleteCommandBuilder(Delete<T> delete, Dialect dialect) {
		this.delete = delete;
		this.dialect = dialect;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider(dialect.getDmlNameProviderFactory());
	}
	
	@Override
	public String toSQL() {
		StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
		appendDeleteStatement(result, dmlNameProvider);
		return result.getSQL();
	}
	
	@Override
	public ExpandableSQLAppender toPreparableSQL() {
		// We ask for SQL generation through a ExpandableSQLAppender because we need SQL placeholders for where + update clause
		ExpandableSQLAppender preparedSQLAppender = new ExpandableSQLAppender(dialect.getColumnBinderRegistry(), dmlNameProvider);
		appendDeleteStatement(preparedSQLAppender, dmlNameProvider);
		
		return preparedSQLAppender;
	}
	
	private void appendDeleteStatement(SQLAppender target, MultiTableAwareDMLNameProvider dmlNameProvider) {
		target.cat("delete from ");
		
		// looking for additional Tables : more than the updated one, can be found in conditions
		Set<Column<Table<?>, Object>> whereColumns = new LinkedHashSet<>();
		delete.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion && ((ColumnCriterion) c).getColumn() instanceof Column) {
				whereColumns.add((Column<Table<?>, Object>) ((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof UnitaryOperator
						&& ((UnitaryOperator) condition).getValue() instanceof ValuedVariable
						&& ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue() instanceof Column) {
					whereColumns.add((Column) ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue());
				}
			}
		});
		Collection<? extends Table<?>> tablesInCondition = Iterables.collect(whereColumns, Column::getTable, HashSet::new);
		tablesInCondition.remove(this.delete.getTargetTable());
		Collection<? extends Table<?>> additionalTables = tablesInCondition;
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		target.cat(this.delete.getTargetTable().getAbsoluteName())    // main table is always referenced with name (not alias)
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<? extends Table<?>> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			target.cat(next.getAbsoluteName()).catIf(iterator.hasNext(), ", ");
		}
		
		
		// append where clause
		if (delete.getCriteria().iterator().hasNext()) {
			target.cat(" where ");
			WhereSQLBuilder whereSqlBuilder = dialect.getQuerySQLBuilderFactory().getWhereBuilderFactory().whereBuilder(this.delete.getCriteria(), dmlNameProvider, dialect.getQuerySQLBuilderFactory());
			whereSqlBuilder.appendTo(target);
		}
	}
	
	public DeleteStatement<T> toStatement() {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		MutableInt variableCounter = new MutableInt();
		Map<String, Set<Integer>> placeholderIndexes = new HashMap<>();
		
		PreparedSQLAppender statementAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry()) {
			
			@Override
			public <V> PreparedSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
				PreparedSQLAppender result = super.catValue(column, value);
				if (value instanceof Placeholder) {
					placeholderIndexes.computeIfAbsent(((Placeholder) value).getName(), name -> new HashSet<>())
							.add(variableCounter.increment());
				}
				return result;
			}
		};
		appendDeleteStatement(statementAppender, dmlNameProvider);
		
		// final assembly
		Map<Integer, Object> values = new HashMap<>(statementAppender.getValues());
		Map<Integer, ParameterBinder<?>> parameterBinders = statementAppender.getParameterBinders();
		
		Iterator<PlaceholderVariable<?, T>> placeholderIterator = delete.getRow().iterator();
		while (placeholderIterator.hasNext()) {
			PlaceholderVariable<?, T> c = placeholderIterator.next();
			Set<Integer> indexes = placeholderIndexes.get(c.getName());
			if (indexes == null) {
				throw new IllegalArgumentException("No placeholder named \"" + c.getName() + "\" found in statement, available are "
						+ placeholderIndexes.keySet());
			}
			indexes.forEach(index -> values.put(index, c.getValue()));
		}
		
		DeleteStatement<T> result = new DeleteStatement<>(
				statementAppender.getSQL(),
				parameterBinders,
				placeholderIndexes);
		result.setValues(values);
		return result;
	}
	
	/**
	 * A specialized version of {@link PreparedSQL} dedicated to {@link Delete} so one can set column values of the where clause
	 * through {@link #setValue(String, Object)} and make it {@link Delete} "reusable".
	 * Here is a usage example:
	 * <pre>{@code
	 * UpdateStatement updateStatement = new UpdateCommandBuilder(this).toStatement(dialect.getColumnBinderRegistry());
	 * try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(updateStatement, connectionProvider)) {
	 *     writeOperation.setValues(updateStatement.getValues());
	 *     writeOperation.execute();
	 * }
	 * // eventually change some values and re-execute it
	 * updateStatement.setValue(..);
	 * }</pre>
	 */
	public static class DeleteStatement<T extends Table<T>> extends PreparedSQL implements WherableStatement {
		
		private final Map<String, Set<Integer>> placeholderIndexes;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link DeleteCommandBuilder}.
		 *
		 * @param sql the update sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param placeholderIndexes indexes of variables of criteria clause
		 */
		public DeleteStatement(String sql,
							   Map<Integer, ? extends ParameterBinder<?>> parameterBinders,
							   Map<String, Set<Integer>> placeholderIndexes) {
			super(sql, parameterBinders);
			this.placeholderIndexes = placeholderIndexes;
		}
		
		@Override
		public void assertValuesAreApplyable() {
			super.assertValuesAreApplyable();
			Set<Placeholder> presentPlaceholders = getValues().values().stream()
					.filter(Placeholder.class::isInstance)
					.map(Placeholder.class::cast)
					.collect(Collectors.toSet());
			if (!presentPlaceholders.isEmpty()) {
				throw new IllegalStateException("Statement expect values for placeholders: " + presentPlaceholders.stream()
						.map(Placeholder::getName)
						.collect(Collectors.joining(", ")));
			}
		}
		
		@Override
		public <C> void setValue(String placeholderName, C value) {
			Set<Integer> placeholderIndex = placeholderIndexes.get(placeholderName);
			if (placeholderIndex == null) {
				throw new IllegalArgumentException("Placeholder '" + placeholderName + "' is not declared as a criteria in the where clause");
			}
			placeholderIndex.forEach(index -> setValue(index, value));
		}
	}
}
