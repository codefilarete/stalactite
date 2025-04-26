package org.codefilarete.stalactite.engine.crud;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Insert;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultExecutableInsert<T extends Table<T>> extends Insert<T> implements ExecutableInsert<T> {
	
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public DefaultExecutableInsert(T targetTable, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	@Override
	public <C> DefaultExecutableInsert<T> set(Column<? extends T, C> column, C value) {
		super.set(column, value);
		return this;
	}
	
	@Override
	public long execute() {
		InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this, dialect).toStatement();
		try (WriteOperation<Column<T, ?>> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, connectionProvider)) {
			writeOperation.setValues(insertStatement.getValues());
			return writeOperation.execute();
		}
	}
}
