package org.codefilarete.stalactite.engine.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.order.Insert;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder;
import org.codefilarete.stalactite.sql.order.InsertCommandBuilder.InsertStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.function.Predicates.not;

/**
 * 
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultBatchInsert<T extends Table<T>> extends Insert<T> implements BatchInsert<T> {
	
	private final List<Set<InsertColumn<T, ?>>> rows = new ArrayList<>();
	private final Dialect dialect;
	private final ConnectionProvider connectionProvider;
	
	public DefaultBatchInsert(T targetTable, Dialect dialect, ConnectionProvider connectionProvider) {
		super(targetTable);
		this.dialect = dialect;
		this.connectionProvider = connectionProvider;
	}
	
	@Override
	public <C> DefaultBatchInsert<T> set(Column<? extends T, C> column, C value) {
		super.set(column, value);
		return this;
	}
	
	@Override
	public BatchInsert<T> newRow() {
		rows.add(new KeepOrderSet<>(getRow()));
		getRow().clear();
		return this;
	}
	
	@Override
	public long execute() {
		// treating remaining values in case user didn't call newRow(..)
		if (!getRow().isEmpty()) {
			rows.add(new KeepOrderSet<>(getRow()));
		}
		InsertStatement<T> insertStatement = new InsertCommandBuilder<>(this, dialect).toStatement();
		long[] writeCount;
		try (WriteOperation<Column<T, ?>> writeOperation = dialect.getWriteOperationFactory().createInstance(insertStatement, connectionProvider)) {
			this.rows.stream()
					// avoiding empty rows made by several calls to newRow(..) without setting values. Can happen if insert(..) is reused in a loop.
					.filter(not(Set::isEmpty))
					.<Map<Column<T, ?>, ?>>map(row -> Iterables.map(row, InsertColumn::getColumn, InsertColumn::getValue))
					.forEach(writeOperation::addBatch);
			writeCount = writeOperation.executeBatch();
		}
		// we clear current rows to let one reuse this instance
		rows.clear();
		getRow().clear();
		return LongStream.of(writeCount).sum();
	}
}
