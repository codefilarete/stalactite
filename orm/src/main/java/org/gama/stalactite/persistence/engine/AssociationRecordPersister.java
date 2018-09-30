package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * @author Guillaume Mary
 */
public class AssociationRecordPersister<C extends AssociationRecord, T extends AssociationTable> extends Persister<C, C, T> {
	
	public AssociationRecordPersister(
			PersistenceContext persistenceContext,
			ClassMappingStrategy<C, C, T> mappingStrategy) {
		super(persistenceContext, mappingStrategy);
	}
	
	public AssociationRecordPersister(
			ClassMappingStrategy<C, C, T> mappingStrategy,
			Dialect dialect,
			ConnectionProvider connectionProvider,
			int jdbcBatchSize) {
		super(mappingStrategy, dialect, connectionProvider, jdbcBatchSize);
	}
	
	protected AssociationRecordPersister(
			ClassMappingStrategy<C, C, T> mappingStrategy,
			ConnectionProvider connectionProvider,
			DMLGenerator dmlGenerator,
			Retryer writeOperationRetryer,
			int jdbcBatchSize,
			int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
	}
}
