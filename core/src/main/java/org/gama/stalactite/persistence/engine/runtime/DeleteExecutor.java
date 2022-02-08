package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.gama.stalactite.persistence.engine.StaleStateObjectException;
import org.gama.stalactite.persistence.id.assembly.IdentifierAssembler;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.ColumnParameterizedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory.ExpectedBatchedRowCountsSupplier;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> implements org.gama.stalactite.persistence.engine.DeleteExecutor<C, I> {
	
	private SQLOperationListener<Column<T, Object>> operationListener;
	
	public DeleteExecutor(EntityMappingStrategy<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, Object>> listener) {
		this.operationListener = listener;
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 * 
	 * @param entities entites to be deleted
	 * @throws StaleStateObjectException if deleted row count differs from entities count
	 */
	@Override
	public void delete(Iterable<C> entities) {
		ColumnParameterizedSQL<T> deleteStatement = getDmlGenerator().buildDelete(getMappingStrategy().getTargetTable(), getMappingStrategy().getVersionedKeys());
		List<? extends C> entitiesCopy = Iterables.copy(entities);
		ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(entitiesCopy.size(), getBatchSize());
		WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, new CurrentConnectionProvider(), expectedBatchedRowCountsSupplier);
		JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entitiesCopy, writeOperation, getBatchSize());
		jdbcBatchingIterator.forEachRemaining(c -> writeOperation.addBatch(getMappingStrategy().getVersionedKeyValues(c)));
	}
	
	private WriteOperation<Column<T, Object>> newWriteOperation(SQLStatement<Column<T, Object>> statement, ConnectionProvider currentConnectionProvider, LongSupplier expectedRowCount) {
		WriteOperation<Column<T, Object>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
		writeOperation.setListener(operationListener);
		return writeOperation;
	}
	
	private WriteOperation<Column<T, Object>> newWriteOperation(SQLStatement<Column<T, Object>> statement, ConnectionProvider currentConnectionProvider, long expectedRowCount) {
		WriteOperation<Column<T, Object>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
		writeOperation.setListener(operationListener);
		return writeOperation;
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entites to be deleted
	 */
	@Override
	public void deleteById(Iterable<C> entities) {
		// get ids before passing them to deleteFromId
		Set<I> ids = Iterables.collect(entities, getMappingStrategy()::getId, HashSet::new);
		deleteFromId(ids);
	}
	
	/**
	 * Will delete entities only from their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 * 
	 * Can't be named "deleteById" due to generics type erasure that generates same signature as {@link #deleteById(Iterable)}
	 * 
	 * @param ids entities identifiers
	 */
//	@Override
	public void deleteFromId(Iterable<I> ids) {
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
		
		Set<Column<T, Object>> pkColumns = targetTable.getPrimaryKey().getColumns();
		IdentifierAssembler<I> identifierAssembler = getMappingStrategy().getIdMappingStrategy().getIdentifierAssembler();
		if (!parcels.isEmpty()) {
			// creating the eventually tupled order "where (?, ?) in (?, ?)"  
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, blockSize);
			WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider, blockSize);
			JDBCBatchingIterator<List<I>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			// This should stay a List to maintain order between column values and then keep tuple homogeneous for composed id cases
			Map<Column<T, Object>, List<Object>> pkValues = new HashMap<>();
			pkColumns.forEach(c -> pkValues.put(c, new ArrayList<>()));
			// merging all entity ids in a single Map<Column, List> which is given to delete order
			jdbcBatchingIterator.forEachRemaining(deleteKeys -> {
				pkValues.values().forEach(Collection::clear);
				deleteKeys.forEach(id -> identifierAssembler.getColumnValues(id).forEach((c, v) -> pkValues.get(c).add(v)));
				writeOperation.addBatch(pkValues);
			});
		}
		// remaining block treatment
		if (!lastBlock.isEmpty()) {
			deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, lastBlock.size());
			try (WriteOperation<Column<T, Object>> writeOperation = newWriteOperation(deleteStatement, currentConnectionProvider, lastBlock.size())) {
				// we must pass a single value when expected, else ExpandableStatement may be confused when applying them
				Object updateValues = lastBlock.size() == 1 ? lastBlock.get(0) : lastBlock;
				if (updateValues instanceof List) {
					Map<Column<T, Object>, List<Object>> pkValues = new HashMap<>();
					((List<I>) updateValues).forEach(id -> {
						Map<Column<T, Object>, Object> localPkValues = identifierAssembler.getColumnValues(id);
						pkColumns.forEach(pkColumn -> pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>()).add(localPkValues.get(pkColumn)));
					});
					writeOperation.setValues(pkValues);
				} else {
					Map<Column<T, Object>, Object> pkValues = identifierAssembler.getColumnValues((I) updateValues);
					writeOperation.setValues(pkValues);
				}
				writeOperation.execute();
			}
		}
	}
}
