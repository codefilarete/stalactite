package org.codefilarete.stalactite.engine.crud;

import java.util.HashMap;

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableCriteria;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableSQL;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
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
	
	@Override
	public long execute() {
		PreparedSQL deleteStatement = new DeleteCommandBuilder<>(this, dialect).toPreparableSQL().toPreparedSQL(new HashMap<>());
		try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(deleteStatement, connectionProvider)) {
			writeOperation.setValues(deleteStatement.getValues());
			return writeOperation.execute();
		}
	}
	
	@Override
	public ExecutableCriteria where(Column<T, ?> column, String condition) {
		CriteriaChain where = super.where(column, condition);
		return new MethodReferenceDispatcher()
				.redirect(ExecutableSQL::execute, this::execute)
				.redirect(CriteriaChain.class, where, true)
				.fallbackOn(this).build(ExecutableCriteria.class);
	}
	
	@Override
	public ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition) {
		CriteriaChain where = super.where(column, condition);
		return new MethodReferenceDispatcher()
				.redirect(ExecutableSQL::execute, this::execute)
				.redirect(CriteriaChain.class, where, true)
				.fallbackOn(this).build(ExecutableCriteria.class);
	}
}
