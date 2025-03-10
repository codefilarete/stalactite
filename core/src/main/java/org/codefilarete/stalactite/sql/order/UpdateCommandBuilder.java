package org.codefilarete.stalactite.sql.order;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.PreparedSQLAppender;
import org.codefilarete.stalactite.query.builder.SQLAppender;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.builder.StringSQLAppender;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
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
		return toSQL(new StringSQLAppender(dmlNameProvider) {
			@Override
			public <V> StringSQLAppender catValue(@Nullable Selectable<V> column, Object value) {
				if (value == UpdateColumn.PLACEHOLDER) {
					return cat("?");
				} else {
					return super.catValue(column, value);
				}
			}
			
			@Override
			public StringSQLAppender catValue(Object value) {
				if (value == UpdateColumn.PLACEHOLDER) {
					return cat("?");
				} else {
					return super.catValue(value);
				}
			}
		}, dmlNameProvider);
	}
	
	private String toSQL(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		result.cat("update ");
		
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
		
		result.cat(this.update.getTargetTable().getAbsoluteName())    // main table is always referenced with name (not alias)
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<Table> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			result.cat(next.getAbsoluteName()).catIf(iterator.hasNext(), ", ");
		}
		
		// append updated columns part
		result.cat(" set ");
		Iterator<UpdateColumn<T>> columnIterator = update.getColumns().iterator();
		while (columnIterator.hasNext()) {
			UpdateColumn c = columnIterator.next();
			result.cat(dmlNameProvider.getName(c.getColumn()), " = ");
			catUpdateObject(c, result, dmlNameProvider);
			result.catIf(columnIterator.hasNext(), ", ");
		}
		
		// append where clause
		if (!update.getCriteria().getConditions().isEmpty()) {
			result.cat(" where ");
			WhereSQLBuilder whereSqlBuilder = dialect.getQuerySQLBuilderFactory().getWhereBuilderFactory().whereBuilder(this.update.getCriteria(), dmlNameProvider);
			whereSqlBuilder.appendTo(result);
		}
		return result.getSQL();
	}
	
	/**
	 * Method that must append the given value coming from the "set" clause to the given SQLAppender.
	 * Left protected to cover unexpected cases (because {@link UpdateColumn} handle Objects as value)
	 * 
	 * @param updateColumn the value to append in the {@link SQLAppender}
	 * @param result the final SQL appender
	 * @param dmlNameProvider provider of tables and columns names
	 */
	protected void catUpdateObject(UpdateColumn<T> updateColumn, SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		Object value = updateColumn.getValue();
		if (value instanceof Column) {
			// case Update.set(colA, colB)
			result.cat(dmlNameProvider.getName((Column) value));
		} else {
			// case Update.set(colA, any object) and Update.set(colA)  (with UpdateColumn.PLACEHOLDER as value)
			result.catValue(updateColumn.getColumn(), value);
		}
	}
	
	public UpdateStatement<T> toStatement() {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLAppender preparedSQLAppender = new PreparedSQLAppender(new StringSQLAppender(dmlNameProvider), dialect.getColumnBinderRegistry());
		String sql = toSQL(preparedSQLAppender, dmlNameProvider);
		
		Map<Integer, Object> values = new HashMap<>(preparedSQLAppender.getValues());
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>(preparedSQLAppender.getParameterBinders());
		Map<Column<T, Object>, Integer> columnIndexes = new HashMap<>();
		
		// PreparedSQLWrapper has filled values (see catUpdateObject(..)) but PLACEHOLDERs must be removed from them.
		// (ParameterBinders are correctly filled by PreparedSQLWrapper)
		// Moreover we have to build indexes of Columns to allow usage of UpdateStatement.setValue(..)
		// So we iterate of set Columns to remove unnecessary columns and compute column indexes
		MutableInt placeholderColumnCount = new MutableInt();
		update.getColumns().forEach(c -> {
			// only non column value must be adapted (see catUpdateObject(..))
			if (!(c.getValue() instanceof Column)) {
				// NB: prepared statement indexes start at 1 which will be given at first increment
				int index = placeholderColumnCount.increment();
				if (values.get(index).equals(UpdateColumn.PLACEHOLDER)) {
					values.remove(index);
				}
				columnIndexes.put(c.getColumn(), index);
			}
		});
		
		// final assembly
		UpdateStatement<T> result = new UpdateStatement<>(sql, parameterBinders, columnIndexes);
		result.setValues(values);
		return result;
	}
	
	/**
	 * A specialized version of {@link PreparedSQL} dedicated to {@link Update} so one can set column values of the update clause
	 * through {@link #setValue(Column, Object)} and make its {@link Update} "reusable".
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
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 * 
		 * @param sql the update sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param columnIndexes indexes of the updated columns
		 */
		private UpdateStatement(String sql, Map<Integer, ParameterBinder> parameterBinders, Map<? extends Column<T, Object>, Integer> columnIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = (Map<Column<T, Object>, Integer>) columnIndexes;
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
	}
	
}
