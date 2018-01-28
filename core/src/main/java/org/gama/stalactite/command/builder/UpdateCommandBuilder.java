package org.gama.stalactite.command.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.IncrementableInt;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.command.model.Update;
import org.gama.stalactite.command.model.Update.UpdateColumn;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.OperandBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperandBuilder.SQLAppender;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.query.builder.WhereBuilder;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.Operand;

/**
 * A SQL builder for {@link Update} objects
 * 
 * @author Guillaume Mary
 */
public class UpdateCommandBuilder implements SQLBuilder {
	
	private final Update update;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public UpdateCommandBuilder(Update update) {
		this.update = update;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider();
	}
	
	@Override
	public String toSQL() {
		return toSQL(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), dmlNameProvider);
	}
	
	private String toSQL(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		result.cat("update ");
		
		// looking for additionnal Tables : more than the updated one, can be found in conditions
		Set<Column> whereColumns = new LinkedHashSet<>();
		update.getCriteria().getConditions().forEach(c -> {
			if (c instanceof ColumnCriterion) {
				whereColumns.add(((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof Operand && ((Operand) condition).getValue() instanceof Column) {
					whereColumns.add((Column) ((Operand) condition).getValue());
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
		Iterator<UpdateColumn> columnIterator = update.getColumns().iterator();
		while (columnIterator.hasNext()) {
			UpdateColumn c = columnIterator.next();
			result.cat(dmlNameProvider.getName(c.getColumn()), " = ");
			Object value = c.getValue();
			catUpdateObject(value, result, dmlNameProvider);
			if (columnIterator.hasNext()) {
				result.catIf(columnIterator.hasNext(), ", ");
			}
		}
		
		// append where clause
		if (!update.getCriteria().getConditions().isEmpty()) {
			result.cat(" where ");
			WhereBuilder whereBuilder = new WhereBuilder(this.update.getCriteria(), dmlNameProvider);
			whereBuilder.toSQL(result);
		}
		return result.getSQL();
	}
	
	/**
	 * Method that must append the given value coming from the "set" clause to the given SQLAppender.
	 * Let protected to cover unexpected cases (because {@link UpdateColumn} handle Objects as value)
	 * 
	 * @param value the value to append as a String in the {@link SQLAppender}
	 * @param result the final SQL appender
	 * @param dmlNameProvider provider of tables and columns names
	 */
	protected void catUpdateObject(Object value, SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		if (value == UpdateColumn.PLACEHOLDER) {
			result.cat("?");
		} else if (value instanceof Column) {
			result.cat(dmlNameProvider.getName((Column) value));
		} else {
			result.catValue(value);
		}
	}
	
	public UpdateStatement toStatement(ColumnBinderRegistry columnBinderRegistry) {
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>();
		
		// Computing updated columns placeholders ('?') indexes
		Map<Column, Integer> columnIndexes = new HashMap<>();
		IncrementableInt placeholderColumnCount = new IncrementableInt();
		update.getColumns().stream().filter(c -> c.getValue() == UpdateColumn.PLACEHOLDER).forEach(c -> {
			// NB: prepared statement indexes start at 1 which will be given at first increment
			int index = placeholderColumnCount.increment();
			parameterBinders.put(index, columnBinderRegistry.getBinder(c.getColumn()));
			columnIndexes.put(c.getColumn(), index);
		});
		
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), columnBinderRegistry, dmlNameProvider);
		String sql = toSQL(preparedSQLWrapper, dmlNameProvider);
		
		// PreparedSQLWrapperHanger have filled values and parameter binders from the where clause but not from the update part
		// because it doesn't know objects of update clause (UpdateColumn).
		// So we need to shift values and parameter binders indexes of the "updatable column count" (those that expected placeholders)
		// (we could reiterate over update.getColumns() but result is already here through placeholderColumnCount) 
		int shift = placeholderColumnCount.getValue();
		// shifting values and parameter binders 
		Map<Integer, Object> values = new HashMap<>();
		preparedSQLWrapper.getValues().forEach((index, o) -> values.put(index + shift, o));
		preparedSQLWrapper.getParameterBinders().forEach((index, parameterBinder) -> parameterBinders.put(index + shift, parameterBinder));
		
		// final assembly
		UpdateStatement result = new UpdateStatement(sql, parameterBinders, columnIndexes);
		result.setValues(values);
		return result;
	}
	
	/**
	 * A specialized version of {@link PreparedSQL} dedicated to {@link Update} so one can set column values of the update clause
	 * through {@link #setValue(Column, Object)}
	 */
	public static class UpdateStatement extends PreparedSQL {
		
		private final Map<Column, Integer> columnIndexes;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 * 
		 * @param sql the update sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param columnIndexes indexes of the updated columns
		 */
		private UpdateStatement(String sql, Map<Integer, ParameterBinder> parameterBinders, Map<Column, Integer> columnIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = columnIndexes;
		}
		
		/**
		 * Dedicated method for setting values of updated {@link Column}s.
		 * @param column {@link Column} to be set
		 * @param value value applied on Column
		 */
		public void setValue(Column column, Object value) {
			super.setValue(columnIndexes.get(column), value);
		}
	}
	
}
