package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory.ExpectedBatchedRowCountsSupplier;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

import static java.util.Collections.*;
import static org.codefilarete.tool.collection.Iterables.cutTail;

/**
 * Class dedicated to delete statement execution
 * 
 * @author Guillaume Mary
 */
public class DeleteExecutor<C, I, T extends Table<T>> extends WriteExecutor<C, I, T> implements org.codefilarete.stalactite.engine.DeleteExecutor<C, I> {
	
	private SQLOperationListener<Column<T, ?>> operationListener;
	
	public DeleteExecutor(EntityMapping<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, ?>> listener) {
		this.operationListener = listener;
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 * 
	 * @param entities entities to be deleted
	 * @throws StaleStateObjectException if deleted row count differs from entities count
	 */
	@Override
	public void delete(Iterable<? extends C> entities) {
		ColumnParameterizedSQL<T> deleteStatement = getDmlGenerator().buildDelete(getMapping().getTargetTable(), getMapping().getVersionedKeys());
		List<? extends C> entitiesCopy = Iterables.copy(entities);
		ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(entitiesCopy.size(), getBatchSize());
		WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, getConnectionProvider(), expectedBatchedRowCountsSupplier);
		JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entitiesCopy, writeOperation, getBatchSize());
		jdbcBatchingIterator.forEachRemaining(c -> writeOperation.addBatch(getMapping().getVersionedKeyValues(c)));
	}
	
	private WriteOperation<Column<T, ?>> newWriteOperation(SQLStatement<Column<T, ?>> statement, ConnectionProvider currentConnectionProvider, LongSupplier expectedRowCount) {
		WriteOperation<Column<T, ?>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
		writeOperation.setListener(operationListener);
		return writeOperation;
	}
	
	private WriteOperation<Column<T, ?>> newWriteOperation(SQLStatement<Column<T, ?>> statement, ConnectionProvider currentConnectionProvider, long expectedRowCount) {
		WriteOperation<Column<T, ?>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
		writeOperation.setListener(operationListener);
		return writeOperation;
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entities to be deleted
	 */
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		// get ids before passing them to deleteFromId
		Set<I> ids = Iterables.collect(entities, getMapping()::getId, HashSet::new);
		deleteFromId(ids);
	}
	
	/**
	 * Will delete entities only from their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 * 
	 * Can't be named "deleteById" due to generics type erasure that generates same signature as {@link org.codefilarete.stalactite.engine.DeleteExecutor#deleteById(Iterable)}
	 * 
	 * @param ids entities identifiers
	 */
	//@Override
	public void deleteFromId(Iterable<I> ids) {
		int blockSize = getInOperatorMaxSize();
		List<List<I>> parcels = Collections.parcel(ids, blockSize);
		
		if (parcels.isEmpty()) {
			return;
		}
		
		// Extract last block and adjust parcels if needed
		List<I> lastBlock = Iterables.last(parcels, emptyList());
		if (lastBlock.size() == blockSize) {
			// Last block is full-sized, process it with the others
			lastBlock = emptyList();
		} else {
			// Last block is partial, remove it from parcels to process separately
			parcels = cutTail(parcels);
		}
		
		// NB: ConnectionProvider must provide the same connection over all blocks
		ConnectionProvider currentConnectionProvider = getConnectionProvider();
		T targetTable = getMapping().getTargetTable();
		Set<Column<T, ?>> pkColumns = targetTable.getPrimaryKey().getColumns();
		IdentifierAssembler<I, T> identifierAssembler = getMapping().getIdMapping().getIdentifierAssembler();
		
		// Process full-sized blocks
		if (!parcels.isEmpty()) {
			processFullSizedBlocks(parcels, currentConnectionProvider, targetTable, pkColumns, identifierAssembler, blockSize);
		}
		
		// Process remaining partial block if any
		if (!lastBlock.isEmpty()) {
			processPartialBlock(lastBlock, currentConnectionProvider, targetTable, pkColumns, identifierAssembler);
		}
	}
	
	private void processFullSizedBlocks(List<List<I>> parcels,
										ConnectionProvider connectionProvider,
										T targetTable,
										Set<Column<T, ?>> pkColumns,
										IdentifierAssembler<I, T> identifierAssembler, int blockSize) {
		// Creating the eventually tupled order "where (?, ?) in (?, ?)"
		ColumnParameterizedSQL<T> deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, blockSize);
		try (WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, connectionProvider, blockSize)) {
			JDBCBatchingIterator<List<I>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, getBatchSize());
			
			// This should stay a List to maintain order between column values and then keep tuple homogeneous for composed id cases
			Map<Column<T, ?>, List<Object>> pkValues = new HashMap<>();
			pkColumns.forEach(c -> pkValues.put(c, new ArrayList<>()));
			
			// Merging all entity ids in a single Map<Column, List> which is given to delete order
			jdbcBatchingIterator.forEachRemaining(deleteKeys -> {
				pkValues.values().forEach(Collection::clear);
				deleteKeys.forEach(id -> identifierAssembler.getColumnValues(id).forEach((c, v) -> pkValues.get(c).add(v)));
				writeOperation.addBatch(pkValues);
			});
		}
	}
	
	private void processPartialBlock(List<I> lastBlock,
									 ConnectionProvider connectionProvider,
									 T targetTable,
									 Set<Column<T, ?>> pkColumns,
									 IdentifierAssembler<I, T> identifierAssembler) {
		ColumnParameterizedSQL<T> deleteStatement = getDmlGenerator().buildDeleteByKey(targetTable, pkColumns, lastBlock.size());
		try (WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(deleteStatement, connectionProvider, lastBlock.size())) {
			// we must pass a single value when expected, else ExpandableStatement will be confused when applying them and an error will be thrown
			// see ExpandableStatement.adaptIterablePlaceholders(..)
			if (lastBlock.size() == 1) {
				writeOperation.setValues(identifierAssembler.getColumnValues(lastBlock.get(0)));
			} else {
				Map<Column<T, ?>, List<Object>> pkValues = new HashMap<>();
				lastBlock.forEach(id -> {
					Map<Column<T, ?>, Object> localPkValues = identifierAssembler.getColumnValues(id);
					pkColumns.forEach(pkColumn ->
							pkValues.computeIfAbsent(pkColumn, k -> new ArrayList<>()).add(localPkValues.get(pkColumn)));
				});
				writeOperation.setValues(pkValues);
			}
			writeOperation.execute();
		}
	}
}
