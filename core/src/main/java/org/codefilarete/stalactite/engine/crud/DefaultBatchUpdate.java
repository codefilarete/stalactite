package org.codefilarete.stalactite.engine.crud;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.ColumnVariable;
import org.codefilarete.stalactite.sql.order.PlaceholderVariable;
import org.codefilarete.stalactite.sql.order.StatementVariable;
import org.codefilarete.stalactite.sql.order.Update;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder;
import org.codefilarete.stalactite.sql.order.UpdateCommandBuilder.UpdateStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultBatchUpdate<T extends Table<T>> implements BatchUpdate<T> {
	
	private final Update<T> statement;
	private final Deque<Set<? extends StatementVariable<?, T>>> rows = new ArrayDeque<>();
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	private UpdateStatement<T> updateStatement;
	
	public DefaultBatchUpdate(Update<T> statement, Dialect dialect, ConnectionProvider connectionProvider) {
		this.statement = statement;
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
		this.rows.add(statement.getRow());
	}
	
	@Override
	public DefaultBatchUpdate<T> newRow() {
		rows.addLast(new KeepOrderSet<>());
		return this;
	}
	
	/**
	 * Overridden to adapt return type
	 */
	@Override
	public <C> DefaultBatchUpdate<T> set(Column<? extends T, C> column, C value) {
		assertColumnIsInUpdate(column);
		giveCurrentRow().add(new ColumnVariable<>(column, value));
		return this;
	}
	
	@Override
	public <C> BatchUpdate<T> set(String argName, C value) {
		giveCurrentRow().add(new PlaceholderVariable<>(argName, value));
		return this;
	}
	
	private Set<StatementVariable<?, T>> giveCurrentRow() {
		return (Set<StatementVariable<?, T>>) rows.getLast();
	}
	
	private <C> void assertColumnIsInUpdate(Column<? extends T, C> column) {
		if (!statement.getColumnsToUpdate().contains(column)) {
			throw new IllegalArgumentException("Column " + column + " is not defined in this batch update");
		}
	}
	
	/**
	 * Executes this update statement with given values
	 */
	@Override
	public long execute() {
		// because BatchUpdate are reusable we don't recreate the statement each time the execute() method is called
		if (updateStatement == null) {
			updateStatement = new UpdateCommandBuilder<>(this.statement, dialect).toStatement();
		}
		long[] writeCount;
		try (WriteOperation<Integer> writeOperation = dialect.getWriteOperationFactory().createInstance(updateStatement, connectionProvider)) {
			rows.forEach(row -> {
				row.forEach(c -> c.applyValueTo(updateStatement));
				writeOperation.addBatch(updateStatement.getValues());
			});
			writeCount = writeOperation.executeBatch();
		}
		// we clear current rows to let one reuse this instance
		rows.clear();
		rows.add(new KeepOrderSet<>());
		return LongStream.of(writeCount).sum();
	}
}
