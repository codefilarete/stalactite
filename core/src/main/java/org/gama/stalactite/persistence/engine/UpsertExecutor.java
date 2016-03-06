package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * Parent class for Insert and Update statement execution
 * 
 * @author Guillaume Mary
 */
public class UpsertExecutor<T> extends WriteExecutor<T> {
	
	public UpsertExecutor(ClassMappingStrategy<T> mappingStrategy, TransactionManager transactionManager,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, transactionManager, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
}
