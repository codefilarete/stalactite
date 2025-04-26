package org.codefilarete.stalactite.engine.crud;

import java.util.Set;

import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Update;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultExecutableUpdate<T extends Table<T>> extends Update<T> implements ExecutableUpdate<T> {
	
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public DefaultExecutableUpdate(T targetTable, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	public DefaultExecutableUpdate(T targetTable, Where<?> where, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable, where);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	public DefaultExecutableUpdate(T targetTable, Set<? extends Column<T, ?>> columnsToUpdate, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable, columnsToUpdate);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	public DefaultExecutableUpdate(T targetTable, Set<? extends Column<T, ?>> columnsToUpdate, Where<?> where, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable, columnsToUpdate, where);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	/**
	 * Overridden to adapt return type
	 */
	@Override
	public <C> DefaultExecutableUpdate<T> set(Column<? extends T, C> column, C value) {
		super.set(column, value);
		return this;
	}
	
	/**
	 * Overridden to adapt return type
	 */
	@Override
	public <C> DefaultExecutableUpdate<T> set(Column<? extends T, C> column1, Column<?, C> column2) {
		super.set(column1, column2);
		return this;
	}
	
	/**
	 * Executes this update statement with given values
	 */
	@Override
	public long execute() {
		UpdateStatement<T> updateStatement = new UpdateCommandBuilder<>(this, dialect).toStatement();
		try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(updateStatement, connectionProvider)) {
			writeOperation.setValues(updateStatement.getValues());
			return writeOperation.execute();
		}
	}
}
