package org.gama.stalactite.persistence.engine;

import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.*;
import org.gama.sql.IConnectionProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.Persister.IIdentifierFixer;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnPreparedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedSelect;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.sql.result.RowIterator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;
import java.util.Map.Entry;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public class PersisterExecutor<T> {
	
	private final DMLGenerator dmlGenerator;
	private final ClassMappingStrategy<T> mappingStrategy;
	private final int batchSize;
	private final IIdentifierFixer<T> identifierFixer;
	private final TransactionManager transactionManager;
	
	public PersisterExecutor(ClassMappingStrategy<T> mappingStrategy, IIdentifierFixer<T> identifierFixer, int batchSize, TransactionManager transactionManager, ColumnBinderRegistry columnBinderRegistry) {
		this.mappingStrategy = mappingStrategy;
		this.identifierFixer = identifierFixer;
		this.batchSize = batchSize;
		this.transactionManager = transactionManager;
		this.dmlGenerator = new DMLGenerator(columnBinderRegistry);
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public void insert(Iterable<T> iterable) {
		ColumnPreparedSQL insertStatement = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
		WriteOperation<Column> writeOperation = new WriteOperation<>(insertStatement, new ConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation);
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			identifierFixer.fixId(t);
			Map<Column, Object> insertValues = mappingStrategy.getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		/*
		if (identifierGenerator instanceof PostInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * @param iterable iterable of instances
	 */
	public void updateRoughly(Iterable<T> iterable) {
		Table targetTable = mappingStrategy.getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		PreparedUpdate updateOperation = dmlGenerator.buildUpdate(columnsToUpdate, mappingStrategy.getVersionedKeys());
		WriteOperation<UpwhereColumn> writeOperation = new WriteOperation<>(updateOperation, new ConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation);
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<UpwhereColumn, Object> updateValues = mappingStrategy.getUpdateValues(t, null, true);
			writeOperation.addBatch(updateValues);
		}
	}
	
	/**
	 * Update instances that have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 *                            Pass true gives more chance for JDBC batch to be used. 
	 */
	public void update(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		if (allColumnsStatement) {
			updateFully(differencesIterable);
		} else {
			updatePartially(differencesIterable);
		}
	}
	
	/**
	 * Update instances that have changes. Only columns that changed are updated.
	 * Groups statements to benefit from JDBC batch.
	 *
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 */
	public void updatePartially(Iterable<Entry<T, T>> differencesIterable) {
		// Facility to trigger JDBC Batch when number of setted values is reached
		class JDBCBatchingOperation<P> {
			private final WriteOperation<P> writeOperation;
			private long stepCounter = 0;
			
			private JDBCBatchingOperation(WriteOperation<P> writeOperation) {
				this.writeOperation = writeOperation;
			}
			
			private void setValues(Map<P, Object> values) {
				this.writeOperation.addBatch(values);
				this.stepCounter++;
				executeBatchIfNecessary();
			}
			
			private void  executeBatchIfNecessary() {
				if (stepCounter == PersisterExecutor.this.batchSize) {
					writeOperation.executeBatch();
					stepCounter = 0;
				}
			}
		}
		
		final ConnectionProvider connectionProvider = new ConnectionProvider();
		// cache for WriteOperation instances (key is Columns to be updated) for batch use
		Map<Set<UpwhereColumn>, JDBCBatchingOperation<UpwhereColumn>> updateOperationCache = new ValueFactoryHashMap<>(new IFactory<Set<UpwhereColumn>, JDBCBatchingOperation<UpwhereColumn>>() {
			@Override
			public JDBCBatchingOperation<UpwhereColumn> createInstance(Set<UpwhereColumn> input) {
				PreparedUpdate preparedUpdate = dmlGenerator.buildUpdate(UpwhereColumn.getUpdateColumns(input), mappingStrategy.getVersionedKeys());
				return new JDBCBatchingOperation<>(new WriteOperation<>(preparedUpdate, connectionProvider));
			}
		});
		// building UpdateOperations and update values
		for (Entry<T, T> next : differencesIterable) {
			T modified = next.getKey();
			T unmodified = next.getValue();
			// finding differences between modified instances and unmodified ones
			Map<UpwhereColumn, Object> updateValues = mappingStrategy.getUpdateValues(modified, unmodified, false);
			if (!updateValues.isEmpty()) {
				JDBCBatchingOperation<UpwhereColumn> writeOperation = updateOperationCache.get(updateValues.keySet());
				writeOperation.setValues(updateValues);
			} // else nothing to do (no modification)
		}
		// treating remaining values not yet executed
		for (JDBCBatchingOperation<UpwhereColumn> entryUpwhereColumnJDBCBatchingOperation : updateOperationCache.values()) {
			if (entryUpwhereColumnJDBCBatchingOperation.stepCounter != 0) {
				entryUpwhereColumnJDBCBatchingOperation.writeOperation.executeBatch();
			}
		}
	}
	
	/**
	 * Update instances that have changes. All columns are updated.
	 * Groups statements to benefit from JDBC batch.
	 * 
	 * @param differencesIterable iterable of instances
	 */
	public void updateFully(Iterable<Entry<T, T>> differencesIterable) {
		ConnectionProvider connectionProvider = new ConnectionProvider();
		PreparedUpdate preparedUpdate = dmlGenerator.buildUpdate(mappingStrategy.getUpdatableColumns(), mappingStrategy.getVersionedKeys());
		WriteOperation<UpwhereColumn> writeOperation = new WriteOperation<>(preparedUpdate, connectionProvider);
		
		// Since all columns are updated we can benefit from JDBC batch
		JDBCBatchingIterator<Entry<T, T>> jdbcBatchingIterator = new JDBCBatchingIterator<>(differencesIterable, writeOperation);
		while(jdbcBatchingIterator.hasNext()) {
			Entry<T, T> entry = jdbcBatchingIterator.next();
			T modified = entry.getKey();
			T unmodified = entry.getValue();
			// finding differences between modified instance and unmodified one
			Map<UpwhereColumn, Object> updateValues = mappingStrategy.getUpdateValues(modified, unmodified, true);
			if (!updateValues.isEmpty()) {
				writeOperation.addBatch(updateValues);
			} // else nothing to do (no modification)
		}
	}
	
	public void delete(Iterable<T> iterable) {
		// Check if delete can be optimized
		if (mappingStrategy.isSingleColumnKey()) {
			deleteRoughly(iterable);
		} else {
			ColumnPreparedSQL deleteStatement = dmlGenerator.buildDelete(mappingStrategy.getTargetTable(), mappingStrategy.getVersionedKeys());
			WriteOperation<Column> writeOperation = new WriteOperation<>(deleteStatement, new ConnectionProvider());
			JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation);
			while(jdbcBatchingIterator.hasNext()) {
				T t = jdbcBatchingIterator.next();
				writeOperation.addBatch(mappingStrategy.getDeleteValues(t));
			}
		}
	}
	
	public void deleteRoughly(Iterable<T> iterable) {
		if (!mappingStrategy.isSingleColumnKey()) {
			throw new UnsupportedOperationException("Roughly delete is only supported with single column key");
		}
		// get ids before passing them to deleteRoughlyById
		List<Serializable> ids = new ArrayList<>();
		for (T t : iterable) {
			ids.add(mappingStrategy.getId(t));
		}
		deleteRoughlyById(ids);
	}
	
	public void deleteRoughlyById(Iterable<Serializable> ids) {
		// NB: ConnectionProvider must provide the same connection over all blocks
		ConnectionProvider connectionProvider = new ConnectionProvider();
		int blockSize = 3;
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		ColumnPreparedSQL deleteStatement;
		WriteOperation<Column> writeOperation;
		Table targetTable = mappingStrategy.getTargetTable();
		Column keyColumn = mappingStrategy.getSingleColumnKey();
		if (parcels.size() > 1) {
			deleteStatement = dmlGenerator.buildMassiveDelete(targetTable, keyColumn, blockSize);
			writeOperation = new WriteOperation<>(deleteStatement, connectionProvider);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			JDBCBatchingIterator<List<Serializable>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation);
			while(jdbcBatchingIterator.hasNext()) {
				List<Serializable> updateValues = jdbcBatchingIterator.next();
				writeOperation.addBatch(Maps.asMap(keyColumn, (Object) updateValues));
			}
		}
		if (lastBlock.size() > 0) {
			deleteStatement = dmlGenerator.buildMassiveDelete(targetTable, keyColumn, lastBlock.size());
			writeOperation = new WriteOperation<>(deleteStatement, connectionProvider);
			writeOperation.setValue(keyColumn, lastBlock);
			writeOperation.execute();
		}
	}
	
	public List<T> select(Iterable<Serializable> ids) {
		ConnectionProvider connectionProvider = new ConnectionProvider();
		List<T> toReturn = new ArrayList<>(50);
		int blockSize = 3;
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		PreparedSelect selectStatement;
		ReadOperation<Column> readOperation;
		Table targetTable = mappingStrategy.getTargetTable();
		Set<Column> columnsToRead = targetTable.getColumns().asSet();
		if (parcels.size() > 1) {
			selectStatement = dmlGenerator.buildMassiveSelect(targetTable, columnsToRead, mappingStrategy.getSingleColumnKey(), blockSize);
			readOperation = new ReadOperation<>(selectStatement, connectionProvider);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			for (List<Serializable> parcel : parcels) {
				toReturn.addAll(execute(readOperation, mappingStrategy.getSingleColumnKey(), parcel));
			}
		}
		if (lastBlock.size() > 0) {
			selectStatement = dmlGenerator.buildMassiveSelect(targetTable, columnsToRead, mappingStrategy.getSingleColumnKey(), lastBlock.size());
			readOperation = new ReadOperation<>(selectStatement, connectionProvider);
			toReturn.addAll(execute(readOperation, mappingStrategy.getSingleColumnKey(), lastBlock));
		}
		return toReturn;
	}
	
	protected List<T> execute(ReadOperation<Column> operation, Column column, Collection<Serializable> values) {
		List<T> toReturn = new ArrayList<>(values.size());
		operation.setValue(column, values);
		ResultSet resultSet = operation.execute();
		RowIterator rowIterator = new RowIterator(resultSet, ((PreparedSelect) operation.getSqlStatement()).getSelectParameterBinders());
		while(rowIterator.hasNext()) {
			toReturn.add(mappingStrategy.transform(rowIterator.next()));
		}
		return toReturn;
	}
	
	private class JDBCBatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		
		public JDBCBatchingIterator(Iterable<E> iterable, WriteOperation writeOperation) {
			super(iterable, PersisterExecutor.this.batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			writeOperation.executeBatch();
		}
	}
	
	private class ConnectionProvider implements IConnectionProvider {
		
		private final Connection currentConnection;
		
		public ConnectionProvider() {
			currentConnection = transactionManager.getCurrentConnection();
		}
		
		@Override
		public Connection getConnection() {
			return currentConnection;
		}
	}
}
