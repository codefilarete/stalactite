package org.gama.stalactite.persistence.engine;

import java.util.Map;

import org.gama.lang.collection.SteppingIterator;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.command.builder.InsertCommandBuilder;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class InsertCommand extends Command<InsertCommand> {
	
	public InsertCommand(org.gama.stalactite.command.model.Insert command, ParameterBinderIndex parameterBinderIndex) {
		super(new InsertCommandBuilder(command).toStatement(parameterBinderIndex));
	}
	
	public int execute(ConnectionProvider connectionProvider, int batchSize) {
		WriteOperation<Column> writeOperation = new WriteOperation<>(getParamedSQL(), connectionProvider);
		JDBCBatchingIterator<Map<Column, Object>, Column> jdbcBatchingIterator = new JDBCBatchingIterator<>(getValues(), writeOperation, batchSize);
		jdbcBatchingIterator.forEachRemaining(columnObjectMap -> jdbcBatchingIterator.getWriteOperation().addBatch(columnObjectMap));
		return jdbcBatchingIterator.getRowCount();
	}
	
	/**
	 * Iterator that triggers batch execution every batch size step.
	 * Usefull for insert and delete statements.
	 */
	public static class JDBCBatchingIterator<E, P> extends SteppingIterator<E> {
		private final WriteOperation<P> writeOperation;
		private int rowCount;
		
		public JDBCBatchingIterator(Iterable<E> iterable, WriteOperation<P> writeOperation, int batchSize) {
			super(iterable, batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			this.rowCount += writeOperation.executeBatch();
		}
		
		public WriteOperation<P> getWriteOperation() {
			return writeOperation;
		}
		
		public int getRowCount() {
			return this.rowCount;
		}
	}
}
