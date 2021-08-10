package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.RowCountManager;
import org.gama.stalactite.persistence.engine.RowCountManager.RowCounter;
import org.gama.stalactite.persistence.engine.StaleObjectExcepion;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.ColumnParameterizedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> implements org.gama.stalactite.persistence.engine.DeleteExecutor<C, I> {
	
	private RowCountManager rowCountManager = RowCountManager.THROWING_ROW_COUNT_MANAGER;
	
	private SQLOperationListener<Column<T, Object>> operationListener;
	
	public DeleteExecutor(EntityMappingStrategy<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
	}
	
	public void setRowCountManager(RowCountManager rowCountManager) {
		this.rowCountManager = rowCountManager;
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, Object>> listener) {
		this.operationListener = listener;
	}
	
	private WriteOperation<Column<T, Object>> newWriteOperation(SQLStatement<Column<T, Object>> statement, CurrentConnectionProvider currentConnectionProvider) {
		WriteOperation<Column<T, Object>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider);
		writeOperation.setListener(operationListener);
		return writeOperation;
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 * 
	 * @param entities entites to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	@Override
	public int delete(Iterable<C> entities) {
		ColumnParameterizedSQL<T> deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
		WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, new CurrentConnectionProvider());
		JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entities, writeOperation, getBatchSize());
		RowCounter rowCounter = new RowCounter();
		while(jdbcBatchingIterator.hasNext()) {
			C c = jdbcBatchingIterator.next();
			Map<Column<T, Object>, Object> versionedKeyValues = getMappingStrategy().getVersionedKeyValues(c);
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
	@Override
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
//	@Override
	public int deleteFromId(Iterable<I> ids) {
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		List<I> lastBlock = Iterables.last(parcels, java.util.Collections.emptyList());
		// Adjusting parcels and last block to group parcels by blockSize
		if (lastBlock.size() != blockSize) {
			parcels = parcels.subList(0, parcels.size() - 1);
		} else {
			lastBlock = java.util.Collections.emptyList();
		}
		
		// NB: CurrentConnectionProvider must provide the same connection over all blocks
		CurrentConnectionProvider currentConnectionProvider = new CurrentConnectionProvider();
		ColumnParameterizedSQL<T> deleteStatement;
		T targetTable = getMappingStrategy().getTargetTable();
		
		int updatedRowCounter = 0;
		Set<Column<T, Object>> pkColumns = targetTable.getPrimaryKey().getColumns();
		IdentifierAssembler<I> identifierAssembler = getMappingStrategy().getIdMappingStrategy().getIdentifierAssembler();
		if (!parcels.isEmpty()) {
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, blockSize);
			WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider);
			JDBCBatchingIterator<List<I>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			while(jdbcBatchingIterator.hasNext()) {
				List<I> updateValues = jdbcBatchingIterator.next();
				Map<Column<T, Object>, List<Object>> pkValues = new HashMap<>();
				updateValues.forEach(id -> {
					Map<Column<T, Object>, Object> localPkValues = identifierAssembler.getColumnValues(id);
					pkColumns.forEach(pkColumn -> pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>()).add(localPkValues.get(pkColumn)));
				});
				writeOperation.addBatch(pkValues);
			}
			updatedRowCounter = jdbcBatchingIterator.getUpdatedRowCount();
		}
		// remaining block treatment
		if (!lastBlock.isEmpty()) {
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, lastBlock.size());
			int updatedRowCount;
			try (WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider)) {
				// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
				Object updateValues = lastBlock.size() == 1 ? lastBlock.get(0) : lastBlock;
				if (updateValues instanceof List) {
					Map<Column<T, Object>, List<Object>> pkValues = new HashMap<>();
					((List<I>) updateValues).forEach(id -> {
						Map<Column<T, Object>, Object> localPkValues = identifierAssembler.getColumnValues(id);
						pkColumns.forEach(pkColumn -> pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>()).add(localPkValues.get(pkColumn)));
					});
					writeOperation.setValues(pkValues);
					updatedRowCount = writeOperation.execute();
				} else {
					Map<Column<T, Object>, Object> pkValues = identifierAssembler.getColumnValues((I) updateValues);
					writeOperation.setValues(pkValues);
					updatedRowCount = writeOperation.execute();
				}
			}
			updatedRowCounter += updatedRowCount;
		}
		return updatedRowCounter;
	}
}
