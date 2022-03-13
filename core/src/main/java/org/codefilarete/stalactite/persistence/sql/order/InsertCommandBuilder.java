package org.codefilarete.stalactite.persistence.sql.order;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.persistence.sql.order.Update.UpdateColumn;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.PreparedSQLWrapper;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.SQLAppender;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.codefilarete.stalactite.query.builder.SQLBuilder;

/**
 * A SQL builder for {@link Insert} objects
 * Can hardly be mutualized with {@link org.codefilarete.stalactite.persistence.sql.statement.DMLGenerator} because this class provides
 * {@link InsertStatement} which let caller reuse it by setting several time its value through {@link InsertStatement#setValue(Column, Object)}
 * 
 * @author Guillaume Mary
 */
public class InsertCommandBuilder<T extends Table> implements SQLBuilder {
	
	private final Insert<T> insert;
	
	public InsertCommandBuilder(Insert<T> insert) {
		this.insert = insert;
	}
	
	@Override
	public String toSQL() {
		return toSQL(new StringAppenderWrapper(new StringAppender(), new DMLNameProvider(new HashMap<>())) {
			@Override
			public StringAppenderWrapper catValue(Column column, Object value) {
				if (value == UpdateColumn.PLACEHOLDER) {
					return cat("?");
				} else {
					return super.catValue(column, value);
				}
			}
		});
	}
	
	private String toSQL(SQLAppender result) {
		result.cat("insert into ").cat(insert.getTargetTable().getAbsoluteName())
		.cat("(");
		
		Iterator<UpdateColumn> columnIterator = insert.getColumns().iterator();
		while (columnIterator.hasNext()) {
			UpdateColumn c = columnIterator.next();
			result.cat(c.getColumn().getName());
			if (columnIterator.hasNext()) {
				result.catIf(columnIterator.hasNext(), ", ");
			}
		}
		
		result.cat(") values (");
		
		Iterator<UpdateColumn> columnIterator2 = insert.getColumns().iterator();
		while (columnIterator2.hasNext()) {
			UpdateColumn c = columnIterator2.next();
			catUpdateObject(c, result);
			if (columnIterator2.hasNext()) {
				result.catIf(columnIterator2.hasNext(), ", ");
			}
		}
		
		result.cat(")");
		
		return result.getSQL();
	}
	
	/**
	 * Method that must append the given value coming from the "set" clause to the given SQLAppender.
	 * Let protected to cover unexpected cases (because {@link UpdateColumn} handle Objects as value)
	 *
	 * @param value the value to append as a String in the {@link SQLAppender}
	 * @param result the final SQL appender
	 */
	protected void catUpdateObject(UpdateColumn value, SQLAppender result) {
		result.catValue(value.getColumn(), value.getValue());
	}
	
	public InsertStatement<T> toStatement(ColumnBinderRegistry columnBinderRegistry) {
		DMLNameProvider dmlNameProvider = new DMLNameProvider(new HashMap<>());
		
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), columnBinderRegistry, dmlNameProvider);
		String sql = toSQL(preparedSQLWrapper);
		
		Map<Integer, Object> values = new HashMap<>(preparedSQLWrapper.getValues());
		Map<Integer, ParameterBinder> parameterBinders = new HashMap<>(preparedSQLWrapper.getParameterBinders());
		Map<Column<T, Object>, Integer> columnIndexes = new HashMap<>();
		
		// PreparedSQLWrapper has filled values (see catUpdateObject(..)) but PLACEHOLDERs must be removed from them.
		// (ParameterBinders are correctly filled by PreparedSQLWrapper)
		// Moreover we have to build indexes of Columns to allow usage of UpdateStatement.setValue(..)
		// So we iterate of set Columns to remove unnecessary columns and compute column indexes
		ModifiableInt placeholderColumnCount = new ModifiableInt();
		insert.getColumns().forEach(c -> {
			// only non column value must be adapted (see catUpdateObject(..))
			if (!(c.getValue() instanceof Column)) {
				// NB: prepared statement indexes start at 1 which will be given at first increment
				int index = placeholderColumnCount.increment();
				if (values.get(index).equals(UpdateColumn.PLACEHOLDER)) {
					values.remove(index);
				}
				columnIndexes.put((Column<T, Object>) c.getColumn(), index);
			}
		});
		
		// final assembly
		InsertStatement<T> result = new InsertStatement<>(sql, parameterBinders, columnIndexes);
		result.setValues(values);
		return result;
	}
	
	/**
	 * A specialized version of {@link PreparedSQL} dedicated to {@link Insert} so one can set column values of the insert clause
	 * through {@link #setValue(Column, Object)}.
	 * Here is a usage example:
	 * <pre>{@code
	 * InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this).toStatement(dialect.getColumnBinderRegistry());
	 * try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, connectionProvider)) {
	 *     writeOperation.setValues(insertStatement.getValues());
	 *     writeOperation.execute();
	 * }
	 * // eventually change some values and re-execute it
	 * insertStatement.setValue(..);
	 * }</pre>
	 */
	public static class InsertStatement<T extends Table> extends PreparedSQL {
		
		private final Map<? extends Column<T, Object>, Integer> columnIndexes;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 *
		 * @param sql the insert sql order as a prepared statement
		 * @param parameterBinders binder for prepared statement values
		 * @param columnIndexes indexes of the updated columns
		 */
		private InsertStatement(String sql, Map<Integer, ParameterBinder> parameterBinders, Map<? extends Column<T, Object>, Integer> columnIndexes) {
			super(sql, parameterBinders);
			this.columnIndexes = columnIndexes;
		}
		
		/**
		 * Dedicated method to set values of inserted {@link Column}s.
		 *
		 * @param column {@link Column} to be set
		 * @param value value applied on Column
		 */
		public <C> void setValue(Column<T, C> column, C value) {
			if (columnIndexes.get(column) == null) {
				throw new IllegalArgumentException("Column " + column.getAbsoluteName() + " is not insertable with fixed value in the insert clause");
			}
			setValue(columnIndexes.get(column), value);
		}
	}
}
