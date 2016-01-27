package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<T> extends WriteExecutor<T> {
	
	public DeleteExecutor(ClassMappingStrategy<T> mappingStrategy, Persister.IIdentifierFixer<T> identifierFixer,
						  TransactionManager transactionManager, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, identifierFixer, transactionManager, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public int delete(Iterable<T> iterable) {
		// Check if delete can be optimized
		if (getMappingStrategy().isSingleColumnKey()) {
			return deleteRoughly(iterable);
		} else {
			ColumnParamedSQL deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
			WriteOperation<Table.Column> writeOperation = newWriteOperation(deleteStatement, new ConnectionProvider());
			JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
			while(jdbcBatchingIterator.hasNext()) {
				T t = jdbcBatchingIterator.next();
				writeOperation.addBatch(getMappingStrategy().getDeleteValues(t));
			}
			return jdbcBatchingIterator.getUpdatedRowCount();
		}
	}
	
	public int deleteRoughly(Iterable<T> iterable) {
		if (!getMappingStrategy().isSingleColumnKey()) {
			throw new UnsupportedOperationException("Roughly delete is only supported with single column key");
		}
		// get ids before passing them to deleteRoughlyById
		List<Serializable> ids = new ArrayList<>();
		for (T t : iterable) {
			ids.add(getMappingStrategy().getId(t));
		}
		return deleteRoughlyById(ids);
	}
	
	public int deleteRoughlyById(Iterable<Serializable> ids) {
		// NB: ConnectionProvider must provide the same connection over all blocks
		ConnectionProvider connectionProvider = new ConnectionProvider();
		int blockSize = getInOperatorMaxSize();
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		ColumnParamedSQL deleteStatement;
		WriteOperation<Table.Column> writeOperation;
		Table targetTable = getMappingStrategy().getTargetTable();
		Table.Column keyColumn = getMappingStrategy().getSingleColumnKey();
		int updatedRowCounter = 0;
		if (parcels.size() > 1) {
			deleteStatement = getDmlGenerator().buildMassiveDelete(targetTable, keyColumn, blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			writeOperation = newWriteOperation(deleteStatement, connectionProvider);
			JDBCBatchingIterator<List<Serializable>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			while(jdbcBatchingIterator.hasNext()) {
				List<Serializable> updateValues = jdbcBatchingIterator.next();
				writeOperation.addBatch(Maps.asMap(keyColumn, (Object) updateValues));
			}
			updatedRowCounter = jdbcBatchingIterator.getUpdatedRowCount();
		}
		// remaining block treatment
		if (lastBlock.size() > 0) {
			deleteStatement = getDmlGenerator().buildMassiveDelete(targetTable, keyColumn, lastBlock.size());
			writeOperation = newWriteOperation(deleteStatement, connectionProvider);
			writeOperation.setValue(keyColumn, lastBlock);
			int updatedRowCount = writeOperation.execute();
			updatedRowCounter += updatedRowCount;
		}
		return updatedRowCounter;
	}
}
