package org.codefilarete.stalactite.persistence.engine.runtime;

import org.codefilarete.tool.collection.SteppingIterator;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.persistence.sql.dml.DMLGenerator;
import org.codefilarete.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.dml.WriteOperation;

/**
 * Parent class for insert, update or delete executor
 * 
 * @author Guillaume Mary
 */
public abstract class WriteExecutor<C, I, T extends Table> extends DMLExecutor<C, I, T> {
	
	private final int batchSize;
	private final WriteOperationFactory writeOperationFactory;
	
	public WriteExecutor(EntityMappingStrategy<C, I, T> mappingStrategy,
						 ConnectionConfiguration connectionConfiguration,
						 DMLGenerator dmlGenerator,
						 WriteOperationFactory writeOperationFactory,
						 int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration.getConnectionProvider(), dmlGenerator, inOperatorMaxSize);
		this.batchSize = connectionConfiguration.getBatchSize();
		this.writeOperationFactory = writeOperationFactory;
	}
	
	public int getBatchSize() {
		return batchSize;
	}
	
	public WriteOperationFactory getWriteOperationFactory() {
		return writeOperationFactory;
	}
	
	/**
	 * Iterator that triggers batch execution every batch size step.
	 * Usefull for insert and delete statements.
	 */
	public static class JDBCBatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		
		public JDBCBatchingIterator(Iterable<? extends E> entities, WriteOperation writeOperation, int batchSize) {
			super(entities, batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			writeOperation.executeBatch();
		}
		
		public WriteOperation getWriteOperation() {
			return writeOperation;
		}
	}
}