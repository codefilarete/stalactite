package org.codefilarete.stalactite.sql.order;

import java.sql.PreparedStatement;

import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriterIndex;

/**
 * A SQL builder for {@link Insert} objects
 * Can hardly be mutualized with {@link DMLGenerator} because this class provides
 * {@link InsertStatement} which let caller reuse it by setting several time its value through {@link InsertStatement#setValue(Column, Object)}
 * 
 * @author Guillaume Mary
 */
public class InsertCommandBuilder<T extends Table<T>> implements SQLBuilder {
	
	private final Insert<T> insert;
	private final Dialect dialect;
	
	public InsertCommandBuilder(Insert<T> insert, Dialect dialect) {
		this.insert = insert;
		this.dialect = dialect;
	}
	
	@Override
	public String toSQL() {
		ColumnParameterizedSQL<T> columnParameterizedSQL = dialect.getDmlGenerator().buildInsert(insert.getTargetTable().getColumns());
		return columnParameterizedSQL.getSQL();
	}
	
	public InsertStatement<T> toStatement() {
		ColumnParameterizedSQL<T> columnParameterizedSQL = dialect.getDmlGenerator().buildInsert(insert.getTargetTable().getColumns());
		PreparedStatementWriterIndex<Column<T, ?>, PreparedStatementWriter<?>> parameterBinderProvider = columnParameterizedSQL.getParameterBinderProvider();
		InsertStatement<T> result = new InsertStatement<>(parameterBinderProvider, columnParameterizedSQL);
		insert.getColumns().forEach(c -> result.setValue(c.getColumn(), c.getValue()));
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
	public static class InsertStatement<T extends Table<T>> extends SQLStatement<Column<T, ?>> {
		
		private final ColumnParameterizedSQL<T> columnParameterizedSQL;
		
		/**
		 * Single constructor, not expected to be used elsewhere than {@link UpdateCommandBuilder}.
		 *
		 * @param parameterBinderProvider binder for prepared statement values
		 * @param columnParameterizedSQL
		 */
		private InsertStatement(PreparedStatementWriterIndex<Column<T, ?>, PreparedStatementWriter<?>> parameterBinderProvider, ColumnParameterizedSQL<T> columnParameterizedSQL) {
			super(parameterBinderProvider);
			this.columnParameterizedSQL = columnParameterizedSQL;
		}
		
		@Override
		public String getSQL() {
			return columnParameterizedSQL.getSQL();
		}
		
		@Override
		protected void doApplyValue(Column<T, ?> key, Object value, PreparedStatement statement) {
			PreparedStatementWriter<Object> paramBinder = getParameterBinder(key);
			if (paramBinder == null) {
				throw new BindingException("Can't find a " + ParameterBinder.class.getName() + " for column " + key.getAbsoluteName() + " of value " + value
						+ " on sql : " + getSQL());
			}
			doApplyValue(columnParameterizedSQL.getColumnIndexes().get(key)[0], value, paramBinder, statement);
		}
	}
}
