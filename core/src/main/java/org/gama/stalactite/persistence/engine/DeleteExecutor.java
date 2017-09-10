package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.RowCountManager.RowCounter;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<T, I> extends WriteExecutor<T, I> {
	
	private RowCountManager rowCountManager = RowCountManager.THROWING_ROW_COUNT_MANAGER;
	
	public DeleteExecutor(ClassMappingStrategy<T, I> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public void setRowCountManager(RowCountManager rowCountManager) {
		this.rowCountManager = rowCountManager;
	}
	
	/**
	 * Delete instances.
	 * Take optmistic lock into account.
	 * 
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	public int delete(Iterable<T> entities) {
		ColumnParamedSQL deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
		WriteOperation<Table.Column> writeOperation = newWriteOperation(deleteStatement, new CurrentConnectionProvider());
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(entities, writeOperation, getBatchSize());
		RowCounter rowCounter = new RowCounter();
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<Column, Object> versionedKeyValues = getMappingStrategy().getVersionedKeyValues(t);
			writeOperation.addBatch(versionedKeyValues);
			rowCounter.add(versionedKeyValues);
		}
		int updatedRowCount = jdbcBatchingIterator.getUpdatedRowCount();
		rowCountManager.checkRowCount(rowCounter, updatedRowCount);
		return updatedRowCount;
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entites to be deleted
	 * @return deleted row count
	 */
	public int deleteById(Iterable<T> entities) {
		// get ids before passing them to deleteFromId
		List<I> ids = new ArrayList<>();
		for (T t : entities) {
			ids.add(getMappingStrategy().getId(t));
		}
		return deleteFromId(ids);
	}
	
	/**
	 * Will delete entities only from their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 * 
	 * Can't be named "deleteById" due to generics type erasure that generates same signature as {@link #deleteById(Iterable)}
	 * 
	 * @param ids entities identifiers
	 * @return deleted row count
	 */
	public int deleteFromId(Iterable<I> ids) {
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
