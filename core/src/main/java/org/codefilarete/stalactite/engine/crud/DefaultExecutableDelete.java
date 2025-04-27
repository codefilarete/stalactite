package org.codefilarete.stalactite.engine.crud;

import java.util.HashMap;

import org.codefilarete.stalactite.query.model.Where;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultExecutableDelete<T extends Table<T>> extends Delete<T> implements ExecutableDelete<T> {
	
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public DefaultExecutableDelete(T targetTable, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	public DefaultExecutableDelete(T targetTable, Where<?> where, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable, where);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	/**
	 * Overridden to adapt return type
	 */
	@Override
	public <O> DefaultExecutableDelete<T> set(String paramName, O value) {
		super.set(paramName, value);
		return this;
	}
	
	@Override
	public long execute() {
		PreparedSQL deleteStatement = new DeleteCommandBuilder<>(this, dialect).toPreparableSQL().toPreparedSQL(new HashMap<>());
		try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(deleteStatement, connectionProvider)) {
			writeOperation.setValues(deleteStatement.getValues());
			return writeOperation.execute();
		}
	}
}
