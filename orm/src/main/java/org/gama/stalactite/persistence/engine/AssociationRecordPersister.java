package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.sql.ConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * Persister dedicated to record of association table (case of one-to-many association without owning column on target side).
 * Please note whereas 2 DTO exists for indexed and non indexed one-to-many association, there's no 2 dedicated persister because both cases can
 * be completed with some generics, and index is not used by persister class (it is {@link ClassMappingStrategy}'s job)
 * 
 * @author Guillaume Mary
 */
public class AssociationRecordPersister<C extends AssociationRecord, T extends AssociationTable> extends Persister<C, C, T> {
	
	protected AssociationRecordPersister(
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
