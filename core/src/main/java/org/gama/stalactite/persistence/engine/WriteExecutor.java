package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.collection.SteppingIterator;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * Parent class for insert, update or delete executor
 * 
 * @author Guillaume Mary
 */
public abstract class WriteExecutor<T> extends DMLExecutor<T> {
	
	private final int batchSize;
	private final Retryer writeOperationRetryer;
	
	public WriteExecutor(ClassMappingStrategy<T> mappingStrategy,
						 org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						 int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, inOperatorMaxSize);
		this.batchSize = batchSize;
		this.writeOperationRetryer = writeOperationRetryer;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public Retryer getWriteOperationRetryer() {
		return writeOperationRetryer;
	}
	
	protected <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<>(statement, connectionProvider, getWriteOperationRetryer());
	}
	
	/**
	 * Iterator that triggers batch execution every batch size step.
	 * Usefull for insert and delete statements.
	 */
	protected static class JDBCBatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		private int updatedRowCount;
		
		public JDBCBatchingIterator(Iterable<E> iterable, WriteOperation writeOperation, int batchSize) {
			super(iterable, batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			this.updatedRowCount += writeOperation.executeBatch();
		}
		
		public WriteOperation getWriteOperation() {
			return writeOperation;
		}
		
		public int getUpdatedRowCount() {
			return this.updatedRowCount;
		}
	}
}
