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
import org.gama.lang.trace.ModifiableInt;
import org.gama.sql.binder.ParameterBinder;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.command.model.Update;
import org.gama.stalactite.command.model.Update.UpdateColumn;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.OperatorBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperatorBuilder.SQLAppender;
import org.gama.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.query.builder.WhereBuilder;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.UnitaryOperator;

/**
 * A SQL builder for {@link Update} objects
 * 
 * @author Guillaume Mary
 */
public class UpdateCommandBuilder<T extends Table> implements SQLBuilder {
	
	private final Update<T> update;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public UpdateCommandBuilder(Update<T> update) {
		this.update = update;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider();
	}
	
	@Override
	public String toSQL() {
		return toSQL(new StringAppenderWrapper(new StringAppender(), dmlNameProvider) {
			@Override
			public StringAppenderWrapper catValue(Column column, Object value) {
				if (value == UpdateColumn.PLACEHOLDER) {
					return cat("?");
				} else {
					return super.catValue(column, value);
				}
			}
		}, dmlNameProvider);
	}
	
	private String toSQL(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		result.cat("update ");
		
		// looking for additionnal Tables : more than the updated one, can be found in conditions
		Set<Column<T, Object>> whereColumns = new LinkedHashSet<>();
		update.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion) {
				whereColumns.add(((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof UnitaryOperator && ((UnitaryOperator) condition).getValue() instanceof Column) {
					whereColumns.add((Column) ((UnitaryOperator) condition).getValue());
				}
			}
		});
		Set<T> additionalTables = Iterables.minus(
				Iterables.collect(whereColumns, Column::getTable, HashSet::new),
				Arrays.asList(this.update.getTargetTable()));
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		result.cat(this.update.getTargetTable().getAbsoluteName())    // main table is always referenced with name (not alias)
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<T> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			T next = iterator.next();
			result.cat(next.getAbsoluteName()).catIf(iterator.hasNext(), ", ");
		}
		
		// append updated columns part
		result.cat(" set ");
		Iterator<UpdateColumn> columnIterator = update.getColumns().iterator();
		while (columnIterator.hasNext()) {
			UpdateColumn c = columnIterator.next();
			result.cat(dmlNameProvider.getName(c.getColumn()), " = ");
			catUpdateObject(c, result, dmlNameProvider);
			if (columnIterator.hasNext()) {
				result.catIf(columnIterator.hasNext(), ", ");
			}
		}
		
		// append where clause
		if (!update.getCriteria().getConditions().isEmpty()) {
			result.cat(" where ");
			WhereBuilder whereBuilder = new WhereBuilder(this.update.getCriteria(), dmlNameProvider);
			whereBuilder.appendSQL(result);
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
	protected void catUpdateObject(UpdateColumn updateColumn, SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		Object value = updateColumn.getValue();
		if (value instanceof Column) {
			// case Update.set(colA, colB)
			result.cat(dmlNameProvider.getName((Column) value));
		} else {
			// case Update.set(colA, any object) and Update.set(colA)  (with UpdateColumn.PLACEHOLDER as value)
			result.catValue(updateColumn.getColumn(), value);
		}
	}
	
	public UpdateStatement<T> toStatement(ColumnBinderRegistry columnBinderRegistry) {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), columnBinderRegistry, dmlNameProvider);
		String sql = toSQL(preparedSQLWrapper, dmlNameProvider);
		
		Map<Integer, Object> values = new HashMap<>(preparedSQLWrapper.getValues());
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>(preparedSQLWrapper.getParameterBinders());
		Map<Column<T, Object>, Integer> columnIndexes = new HashMap<>();
		
		// PreparedSQLWrapper has filled values (see catUpdateObject(..)) but PLACEHOLDERs must be removed from them.
		// (ParameterBinders are correctly filled by PreparedSQLWrapper)
		// Moreover we have to build indexes of Columns to allow usage of UpdateStatement.setValue(..)
		// So we iterate of set Columns to remove unecessary columns and compute column indexes
		ModifiableInt placeholderColumnCount = new ModifiableInt();
		update.getColumns().forEach(c -> {
			// only non column value must be adapted (see catUpdateObject(..))
			if (!Column.class.isInstance(c.getValue())) {
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
	 * through {@link #setValue(Column, Object)}
	 */
	public static class UpdateStatement<T extends Table> extends PreparedSQL {
		
		private final Map<? extends Column<T, Object>, Integer> columnIndexes;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 * 
		 * @param sql the update sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param columnIndexes indexes of the updated columns
		 */
		private UpdateStatement(String sql, Map<Integer, ParameterBinder> parameterBinders, Map<? extends Column<T, Object>, Integer> columnIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = columnIndexes;
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
