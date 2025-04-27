package org.codefilarete.stalactite.engine.crud;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Delete;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder;
import org.codefilarete.stalactite.sql.order.DeleteCommandBuilder.DeleteStatement;
import org.codefilarete.stalactite.sql.order.PlaceholderVariable;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * 
 * @param <T>
 * @author Guillaume Mary
 */
public class DefaultBatchDelete<T extends Table<T>> implements BatchDelete<T> {
	
	private final Delete<T> statement;
	private final Deque<Set<? extends PlaceholderVariable<?, T>>> rows = new ArrayDeque<>();
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private DeleteStatement<T> deleteStatement;
	
	public DefaultBatchDelete(Delete<T> statement, Dialect dialect, ConnectionProvider connectionProvider) {
		this.statement = statement;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.rows.add(statement.getRow());
	}
	
	@Override
	public DefaultBatchDelete<T> newRow() {
		rows.addLast(new KeepOrderSet<>());
		return this;
	}
	
	@Override
	public <C> DefaultBatchDelete<T> set(String paramName, C value) {
		giveCurrentRow().add(new PlaceholderVariable<>(paramName, value));
		return this;
	}
	
	private Set<PlaceholderVariable<?, T>> giveCurrentRow() {
		return (Set<PlaceholderVariable<?, T>>) rows.getLast();
	}
	
	/**
	 * Executes this update statement with given values
	 */
	@Override
	public long execute() {
		// because BatchDelete are reusable we don't recreate the statement each time the execute() method is called
		if (deleteStatement == null) {
			deleteStatement = new DeleteCommandBuilder<>(this.statement, dialect).toStatement();
		}
		long[] writeCount;
		try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(deleteStatement, connectionProvider)) {
			rows.forEach(row -> {
				row.forEach(c -> c.applyValueTo(deleteStatement));
				writeOperation.addBatch(deleteStatement.getValues());
			});
			writeCount = writeOperation.executeBatch();
		}
		// we clear current rows to let one reuse this instance
		rows.clear();
		rows.add(new KeepOrderSet<>());
		return LongStream.of(writeCount).sum();
	}
}
