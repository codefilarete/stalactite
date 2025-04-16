package org.codefilarete.stalactite.sql.order;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import org.codefilarete.stalactite.sql.order.Update.UpdateColumn;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.tool.function.Functions.chain;

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
	
	private void appendUpdateStatement(SQLAppender setValuesAppender, SQLAppender criteriaAppender, MultiTableAwareDMLNameProvider dmlNameProvider) {
		setValuesAppender.cat("update ");
		
		// looking for additional Tables : more than the updated one, can be found in conditions
		Set<Column<Table, Object>> whereColumns = new LinkedHashSet<>();
		update.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion && ((ColumnCriterion) c).getColumn() instanceof Column) {
				whereColumns.add((Column<Table, Object>) ((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof UnitaryOperator
						&& ((UnitaryOperator) condition).getValue() instanceof ValuedVariable
						&& ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue() instanceof Column) {
					whereColumns.add((Column) ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue());
				}
			}
		});
		Set<Table> additionalTables = Iterables.minus(
				Iterables.collect(whereColumns, Column::getTable, HashSet::new),
				Arrays.asList(this.update.getTargetTable()));
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		setValuesAppender.cat(dmlNameProvider.getName(this.update.getTargetTable()))
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<Table> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			setValuesAppender.cat(dmlNameProvider.getName(next)).catIf(iterator.hasNext(), ", ");
		}
		
		// append updated columns part
		setValuesAppender.cat(" set ");
		Iterator<UpdateColumn<T, ?>> columnIterator = update.getRow().iterator();
		while (columnIterator.hasNext()) {
			UpdateColumn<T, ?> c = columnIterator.next();
			setValuesAppender.cat(dmlNameProvider.getName(c.getColumn()), " = ");
			catUpdateObject(c, setValuesAppender, dmlNameProvider);
			setValuesAppender.catIf(columnIterator.hasNext(), ", ");
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
	 * Left protected to cover unexpected cases (because {@link UpdateColumn} handle Objects as value)
	 * 
	 * @param updateColumn the value to append in the {@link SQLAppender}
	 * @param result the final SQL appender
	 * @param dmlNameProvider provider of tables and columns names
	 */
	protected void catUpdateObject(UpdateColumn<T, ?> updateColumn, SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
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
		Map<Placeholder<T, ?>, Integer> placeholderIndexes = new HashMap<>();
		PreparedSQLAppender setValuesAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry()) {
			
			@Override
			public <V> PreparedSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
				PreparedSQLAppender result = super.catValue(column, value);
				if (value instanceof Placeholder) {
					Placeholder placeholder = (Placeholder) value;
					placeholderIndexes.put(placeholder, variableCounter.increment());
				} else {
					columnIndexes.put((Column) column, variableCounter.increment());
				}
				return result;
			}
		};
		
		PreparedSQLAppender criteriaAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry()) {
			
			@Override
			public <V> PreparedSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
				PreparedSQLAppender result = super.catValue(column, value);
				if (value instanceof Placeholder) {
					Placeholder placeholder = (Placeholder) value;
					placeholderIndexes.put(placeholder, variableCounter.increment());
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
	public static class UpdateStatement<T extends Table<T>> extends PreparedSQL {
		
		private final Map<Column<T, Object>, Integer> columnIndexes;
		private final Map<Placeholder<?, ?>, Integer> placeholderIndexes;
		
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
							   Map<? extends Placeholder<?, ?>, Integer> placeholderIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = (Map<Column<T, Object>, Integer>) columnIndexes;
			this.placeholderIndexes = (Map<Placeholder<?, ?>, Integer>) placeholderIndexes;
		}
		
		/**
		 * Dedicated method to set values of updated {@link Column}s.
		 * 
		 * @param column {@link Column} to be set
		 * @param value value applied on Column
		 */
		public <C> void setValue(Column<T, C> column, C value) {
			Integer index = columnIndexes.get(column);
			if (index == null) {
				throw new IllegalArgumentException("Column " + column.getAbsoluteName() + " is not declared updatable with fixed value in the update clause");
			}
			setValue(index, value);
		}
		
		public <C> void setValue(String placeholderName, C value) {
			Duo<Entry<Placeholder<?, ?>, Integer>, String> placeholderIndex = Iterables.find(placeholderIndexes.entrySet(), chain(Entry::getKey, Placeholder::getName), placeholderName::equals);
			if (placeholderIndex == null) {
				throw new IllegalArgumentException("Placeholder '" + placeholderName + "' is not declared as a criteria in the where clause");
			}
			setValue(placeholderIndex.getLeft().getValue(), value);
		}
	}
	
}
