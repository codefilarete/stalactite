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
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> {
	
	private RowCountManager rowCountManager = RowCountManager.THROWING_ROW_COUNT_MANAGER;
	
	public DeleteExecutor(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public void setRowCountManager(RowCountManager rowCountManager) {
		this.rowCountManager = rowCountManager;
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 * 
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	public int delete(Iterable<C> entities) {
		ColumnParamedSQL<T> deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
		WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, new CurrentConnectionProvider());
		JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entities, writeOperation, getBatchSize());
		RowCounter rowCounter = new RowCounter();
		while(jdbcBatchingIterator.hasNext()) {
			C c = jdbcBatchingIterator.next();
			Map<Column<T, Object>, Object> versionedKeyValues = getMappingStrategy().getVersionedKeyValues(c);
			writeOperation.addBatch((Map) versionedKeyValues);
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
	public int deleteById(Iterable<C> entities) {
		// get ids before passing them to deleteFromId
		List<I> ids = new ArrayList<>();
		for (C c : entities) {
			ids.add(getMappingStrategy().getId(c));
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
		ColumnParamedSQL<T> deleteStatement;
		T targetTable = getMappingStrategy().getTargetTable();
		Column<T, Object> keyColumn = targetTable.getPrimaryKey();
		
		int updatedRowCounter = 0;
		if (parcels.size() > 1) {
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, keyColumn, blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider);
			JDBCBatchingIterator<List<I>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			while(jdbcBatchingIterator.hasNext()) {
				List<I> updateValues = jdbcBatchingIterator.next();
				writeOperation.addBatch(Maps.asMap(keyColumn, updateValues));
			}
			updatedRowCounter = jdbcBatchingIterator.getUpdatedRowCount();
		}
		// remaining block treatment
		if (!lastBlock.isEmpty()) {
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, keyColumn, lastBlock.size());
			int updatedRowCount;
			try (WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider)) {
				// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
				writeOperation.setValue(keyColumn, lastBlock.size() == 1 ? lastBlock.get(0) : lastBlock);
				updatedRowCount = writeOperation.execute();
			}
			updatedRowCounter += updatedRowCount;
		}
		return updatedRowCounter;
	}
}
