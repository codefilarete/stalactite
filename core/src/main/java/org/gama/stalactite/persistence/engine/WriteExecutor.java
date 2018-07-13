package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.collection.SteppingIterator;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Parent class for insert, update or delete executor
 * 
 * @author Guillaume Mary
 */
public abstract class WriteExecutor<C, I, T extends Table> extends DMLExecutor<C, I, T> {
	
	private final int batchSize;
	private final Retryer writeOperationRetryer;
	
	public WriteExecutor(ClassMappingStrategy<C, I, T> mappingStrategy,
						 ConnectionProvider connectionProvider, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
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
	
	protected <P> WriteOperation<P> newWriteOperation(SQLStatement<P> statement, CurrentConnectionProvider currentConnectionProvider) {
		return new WriteOperation<>(statement, currentConnectionProvider, getWriteOperationRetryer());
	}
	
	/**
	 * Iterator that triggers batch execution every batch size step.
	 * Usefull for insert and delete statements.
	 */
	public static class JDBCBatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		private int updatedRowCount;
		
		public JDBCBatchingIterator(Iterable<E> entities, WriteOperation writeOperation, int batchSize) {
			super(entities, batchSize);
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
