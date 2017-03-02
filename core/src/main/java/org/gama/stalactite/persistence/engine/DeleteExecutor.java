package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<T, I> extends WriteExecutor<T, I> {
	
	public DeleteExecutor(ClassMappingStrategy<T, I> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public int delete(Iterable<T> iterable) {
		ColumnParamedSQL deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
		WriteOperation<Table.Column> writeOperation = newWriteOperation(deleteStatement, new CurrentConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			writeOperation.addBatch(getMappingStrategy().getVersionedKeyValues(t));
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	public int deleteRoughly(Iterable<T> iterable) {
		// get ids before passing them to deleteRoughlyById
		List<I> ids = new ArrayList<>();
		for (T t : iterable) {
			ids.add(getMappingStrategy().getId(t));
		}
		return deleteRoughlyById(ids);
	}
	
	public int deleteRoughlyById(Iterable<I> ids) {
		// NB: CurrentConnectionProvider must provide the same connection over all blocks
		CurrentConnectionProvider currentConnectionProvider = new CurrentConnectionProvider();
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<I> lastBlock = Iterables.last(parcels);
		ColumnParamedSQL deleteStatement;
		WriteOperation<Table.Column> writeOperation;
		Table targetTable = getMappingStrategy().getTargetTable();
		Table.Column keyColumn = targetTable.getPrimaryKey();
		
		int updatedRowCounter = 0;
		if (parcels.size() > 1) {
			deleteStatement = getDmlGenerator().buildMassiveDelete(targetTable, keyColumn, blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider);
			JDBCBatchingIterator<List<I>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			while(jdbcBatchingIterator.hasNext()) {
				List<I> updateValues = jdbcBatchingIterator.next();
				writeOperation.addBatch(Maps.asMap(keyColumn, updateValues));
			}
			updatedRowCounter = jdbcBatchingIterator.getUpdatedRowCount();
		}
		// remaining block treatment
		if (lastBlock.size() > 0) {
			deleteStatement = getDmlGenerator().buildMassiveDelete(targetTable, keyColumn, lastBlock.size());
			writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider);
			writeOperation.setValue(keyColumn, lastBlock);
			int updatedRowCount = writeOperation.execute();
			updatedRowCounter += updatedRowCount;
		}
		return updatedRowCounter;
	}
}
