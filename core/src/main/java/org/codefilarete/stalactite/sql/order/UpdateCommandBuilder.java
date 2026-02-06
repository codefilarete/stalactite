package org.codefilarete.stalactite.sql.order;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.MutableInt;

/**
 * A SQL builder for {@link Update} objects.
 * Can hardly be mutualized with {@link DMLGenerator} because the latter doesn't handle multi
 * tables update.
 * 
 * @author Guillaume Mary
 */
public class UpdateCommandBuilder<T extends Table<T>> implements SQLBuilder {
	
	private final Update<T> update;
	private final Dialect dialect;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public UpdateCommandBuilder(Update<T> update, Dialect dialect) {
		this.update = update;
		this.dialect = dialect;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider(dialect.getDmlNameProviderFactory());
	}
	
	@Override
	public String toSQL() {
		StringSQLAppender sqlAppender = new StringSQLAppender(dmlNameProvider);
		appendUpdateStatement(sqlAppender, dmlNameProvider);
		return sqlAppender.getSQL();
	}
	
	private void appendUpdateStatement(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		appendUpdateStatement(result, result, dmlNameProvider);
	}
	
	private void appendUpdateStatement(SQLAppender setClauseAppender, SQLAppender criteriaAppender, MultiTableAwareDMLNameProvider dmlNameProvider) {
		setClauseAppender.cat("update ");
		
		// looking for additional Tables : more than the updated one, can be found in conditions
		Set<Column<Table<?>, Object>> whereColumns = new LinkedHashSet<>();
		update.getCriteria().forEach(c -> {
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
		tablesInCondition.remove(this.update.getTargetTable());
		Collection<? extends Table<?>> additionalTables = tablesInCondition;
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		setClauseAppender.cat(dmlNameProvider.getName(this.update.getTargetTable()))
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<? extends Table<?>> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			setClauseAppender.cat(dmlNameProvider.getName(next)).catIf(iterator.hasNext(), ", ");
		}
		
		// append updated columns part
		setClauseAppender.cat(" set ");
		Iterator<ColumnVariable> columnIterator = update.getRow().stream()
				.filter(ColumnVariable.class::isInstance)
				.map(ColumnVariable.class::cast)
				.iterator();
		while (columnIterator.hasNext()) {
			ColumnVariable<?, T> c = columnIterator.next();
			setClauseAppender.cat(dmlNameProvider.getName(c.getColumn()), " = ");
			appendToSetClause(c, setClauseAppender, dmlNameProvider);
			setClauseAppender.catIf(columnIterator.hasNext(), ", ");
		}
		
		// append where clause
		if (!update.getCriteria().getConditions().isEmpty()) {
			criteriaAppender.cat(" where ");
			WhereSQLBuilder whereSqlBuilder = dialect.getQuerySQLBuilderFactory().getWhereBuilderFactory().whereBuilder(this.update.getCriteria(), dmlNameProvider);
			whereSqlBuilder.appendTo(criteriaAppender);
		}
	}
	
	/**
	 * Method that must append the given value coming from the "set" clause to the given SQLAppender.
	 * Left protected to cover unexpected cases (because {@link InsertColumn} handle Objects as value)
	 * 
	 * @param updateColumn the value to append in the {@link SQLAppender}
	 * @param result the final SQL appender
	 * @param dmlNameProvider provider of tables and columns names
	 */
	protected void appendToSetClause(ColumnVariable<?, T> updateColumn, SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		Object value = updateColumn.getValue();
		if (value instanceof Column) {
			// case Update.set(colA, colB)
			result.cat(dmlNameProvider.getName((Column) value));
		} else {
			// case Update.set(colA, any object)
			result.catValue(updateColumn.getColumn(), value);
		}
	}
	
	public UpdateStatement<T> toStatement() {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		MutableInt variableCounter = new MutableInt();
		Map<Column<T, ?>, Integer> columnIndexes = new HashMap<>();
		Map<String, Set<Integer>> placeholderIndexes = new HashMap<>();
		PreparedSQLAppender setValuesAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry()) {
			
			@Override
			public <V> PreparedSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
				PreparedSQLAppender result = super.catValue(column, value);
				columnIndexes.put((Column) column, variableCounter.increment());
				return result;
			}
		};
		
		PreparedSQLAppender criteriaAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry()) {
			
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
		appendUpdateStatement(setValuesAppender, criteriaAppender, dmlNameProvider);
		
		// final assembly
		Map<Integer, Object> values = new HashMap<>(setValuesAppender.getValues());
		criteriaAppender.getValues().forEach((key, value) -> values.put(key + setValuesAppender.getValues().size(), value));
		Map<Integer, ParameterBinder<?>> parameterBinders = setValuesAppender.getParameterBinders();
		criteriaAppender.getParameterBinders().forEach((key, value) -> parameterBinders.put(key + setValuesAppender.getValues().size(), value));
		
		Iterator<PlaceholderVariable> placeholderIterator = update.getRow().stream()
				.filter(PlaceholderVariable.class::isInstance)
				.map(PlaceholderVariable.class::cast)
				.iterator();
		while (placeholderIterator.hasNext()) {
			PlaceholderVariable<?, T> c = placeholderIterator.next();
			Set<Integer> indexes = placeholderIndexes.get(c.getName());
			if (indexes == null) {
				throw new IllegalArgumentException("No placeholder named \"" + c.getName() + "\" found in statement, available are "
						+ placeholderIndexes.keySet());
			}
			indexes.forEach(index -> values.put(index, c.getValue()));
		}
		
		UpdateStatement<T> result = new UpdateStatement<>(
				setValuesAppender.getSQL() + criteriaAppender.getSQL(),
				parameterBinders,
				columnIndexes,
				placeholderIndexes);
		result.setValues(values);
		return result;
	}
	
	/**
	 * A specialized version of {@link PreparedSQL} dedicated to {@link Update} so one can set column values of the update clause
	 * through {@link #setValue(Column, Object)} and make it {@link Update} "reusable".
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
	public static class UpdateStatement<T extends Table<T>> extends PreparedSQL implements WherableStatement {
		
		private final Map<Column<T, Object>, Integer> columnIndexes;
		private final Map<String, Set<Integer>> placeholderIndexes;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 * 
		 * @param sql the update sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param columnIndexes indexes of the updated columns
		 * @param placeholderIndexes indexes of variables of criteria clause
		 */
		public UpdateStatement(String sql,
							   Map<Integer, ? extends ParameterBinder<?>> parameterBinders,
							   Map<? extends Column<T, ?>, Integer> columnIndexes,
							   Map<String, Set<Integer>> placeholderIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = (Map<Column<T, Object>, Integer>) columnIndexes;
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
		
		/**
		 * Dedicated method to set values of updated {@link Column}s.
		 * 
		 * @param column {@link Column} to be set
		 * @param value value applied on Column
		 */
		public <O> void setValue(Column<T, O> column, O value) {
			Integer index = columnIndexes.get(column);
			if (index == null) {
				throw new IllegalArgumentException("Column " + column.getAbsoluteName() + " is not declared updatable with fixed value in the update clause");
			}
			setValue(index, value);
		}
		
		public <O> void setValue(String placeholderName, O value) {
			Set<Integer> placeholderIndex = placeholderIndexes.get(placeholderName);
			if (placeholderIndex == null) {
				throw new IllegalArgumentException("Placeholder '" + placeholderName + "' is not declared as a criteria in the where clause");
			}
			placeholderIndex.forEach(index -> setValue(index, value));
		}
	}
	
}
