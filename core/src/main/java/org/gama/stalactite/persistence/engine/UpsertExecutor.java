package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Parent class for Insert and Update statement execution
 * 
 * @author Guillaume Mary
 */
public class UpsertExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> {
	
	public UpsertExecutor(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
}
