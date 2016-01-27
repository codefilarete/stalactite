package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.util.Map;

/**
 * Dedicated class to insert statement execution
 * 
 * @author Guillaume Mary
 */
public class InsertExecutor<T> extends WriteExecutor<T> {
	
	public InsertExecutor(ClassMappingStrategy<T> mappingStrategy, Persister.IIdentifierFixer<T> identifierFixer,
						  TransactionManager transactionManager, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, identifierFixer, transactionManager, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public int insert(Iterable<T> iterable) {
		ColumnParamedSQL insertStatement = getDmlGenerator().buildInsert(getMappingStrategy().getTargetTable().getColumns());
		WriteOperation<Table.Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			getIdentifierFixer().fixId(t);
			Map<Table.Column, Object> insertValues = getMappingStrategy().getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
		/*
		if (identifierGenerator instanceof AfterInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
	}
	
}
