package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.bean.IFactory;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.*;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.IConnectionProvider;
import org.gama.sql.dml.ReadOperation;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.sql.result.RowIterator;
import org.gama.stalactite.persistence.engine.Persister.IIdentifierFixer;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnPreparedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedSelect;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
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
	
	private final ClassMappingStrategy<T> mappingStrategy;
	private final IIdentifierFixer<T> identifierFixer;
	private final int batchSize;
	private final TransactionManager transactionManager;
	private final DMLGenerator dmlGenerator;
	private final Retryer writeOperationRetryer;
	private final int inOperatorMaxSize;
	
	public PersisterExecutor(ClassMappingStrategy<T> mappingStrategy, IIdentifierFixer<T> identifierFixer, int batchSize,
							 TransactionManager transactionManager, DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
							 int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.identifierFixer = identifierFixer;
		this.batchSize = batchSize;
		this.transactionManager = transactionManager;
		this.dmlGenerator = dmlGenerator;
		this.writeOperationRetryer = writeOperationRetryer;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public int insert(Iterable<T> iterable) {
		ColumnPreparedSQL insertStatement = dmlGenerator.buildInsert(mappingStrategy.getTargetTable().getColumns());
		WriteOperation<Column> writeOperation = newWriteOperation(insertStatement, new ConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, PersisterExecutor.this.batchSize);
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			identifierFixer.fixId(t);
			Map<Column, Object> insertValues = mappingStrategy.getInsertValues(t);
			writeOperation.addBatch(insertValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
		/*
		if (identifierGenerator instanceof AfterInsertIdentifierGenerator) {
			// TODO: lire le résultat de l'exécution et injecter l'identifiant sur le bean
		}
		*/
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * @param iterable iterable of instances
	 */
	public int updateRoughly(Iterable<T> iterable) {
		Table targetTable = mappingStrategy.getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		PreparedUpdate updateOperation = dmlGenerator.buildUpdate(columnsToUpdate, mappingStrategy.getVersionedKeys());
		WriteOperation<UpwhereColumn> writeOperation = newWriteOperation(updateOperation, new ConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, this.batchSize);
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<UpwhereColumn, Object> updateValues = mappingStrategy.getUpdateValues(t, null, true);
			writeOperation.addBatch(updateValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	/**
	 * Update instances that have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *  @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 */
	public int update(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		if (allColumnsStatement) {
			return updateFully(differencesIterable);
		} else {
			return updatePartially(differencesIterable);
		}
	}
	
	/**
	 * Update instances that have changes. Only columns that changed are updated.
	 * Groups statements to benefit from JDBC batch.
	 *
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 */
	public int updatePartially(Iterable<Entry<T, T>> differencesIterable) {
		ConnectionProvider connectionProvider = new ConnectionProvider();
		return new Updater(new JDBCBatchingOperationCache(connectionProvider), false).update(differencesIterable);
	}
	
	/**
	 * Update instances that have changes. All columns are updated.
	 * Groups statements to benefit from JDBC batch.
	 *
	 * @param differencesIterable iterable of instances
	 */
	public int updateFully(Iterable<Entry<T, T>> differencesIterable) {
		PreparedUpdate preparedUpdate = dmlGenerator.buildUpdate(mappingStrategy.getUpdatableColumns(), mappingStrategy.getVersionedKeys());
		WriteOperation<UpwhereColumn> writeOperation = newWriteOperation(preparedUpdate, new ConnectionProvider());
		// Since all columns are updated we can benefit from JDBC batch
		JDBCBatchingOperation jdbcBatchingOperation = new JDBCBatchingOperation(writeOperation, PersisterExecutor.this.batchSize);
		
		return new Updater(new MonoJDBCBatchingOperation(jdbcBatchingOperation), true).update(differencesIterable);
	}
	
	public int delete(Iterable<T> iterable) {
		// Check if delete can be optimized
		if (mappingStrategy.isSingleColumnKey()) {
			return deleteRoughly(iterable);
		} else {
			ColumnPreparedSQL deleteStatement = dmlGenerator.buildDelete(mappingStrategy.getTargetTable(), mappingStrategy.getVersionedKeys());
			WriteOperation<Column> writeOperation = newWriteOperation(deleteStatement, new ConnectionProvider());
			JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, PersisterExecutor.this.batchSize);
			while(jdbcBatchingIterator.hasNext()) {
				T t = jdbcBatchingIterator.next();
				writeOperation.addBatch(mappingStrategy.getDeleteValues(t));
			}
			return jdbcBatchingIterator.getUpdatedRowCount();
		}
	}
	
	public int deleteRoughly(Iterable<T> iterable) {
		if (!mappingStrategy.isSingleColumnKey()) {
			throw new UnsupportedOperationException("Roughly delete is only supported with single column key");
		}
		// get ids before passing them to deleteRoughlyById
		List<Serializable> ids = new ArrayList<>();
		for (T t : iterable) {
			ids.add(mappingStrategy.getId(t));
		}
		return deleteRoughlyById(ids);
	}
	
	public int deleteRoughlyById(Iterable<Serializable> ids) {
		// NB: ConnectionProvider must provide the same connection over all blocks
		ConnectionProvider connectionProvider = new ConnectionProvider();
		int blockSize = this.inOperatorMaxSize;
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		ColumnPreparedSQL deleteStatement;
		WriteOperation<Column> writeOperation;
		Table targetTable = mappingStrategy.getTargetTable();
		Column keyColumn = mappingStrategy.getSingleColumnKey();
		int updatedRowCounter = 0;
		if (parcels.size() > 1) {
			deleteStatement = dmlGenerator.buildMassiveDelete(targetTable, keyColumn, blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			writeOperation = newWriteOperation(deleteStatement, connectionProvider);
			JDBCBatchingIterator<List<Serializable>> jdbcBatchingIterator = new JDBCBatchingIterator<>(parcels, writeOperation, PersisterExecutor.this.batchSize);
			while(jdbcBatchingIterator.hasNext()) {
				List<Serializable> updateValues = jdbcBatchingIterator.next();
				writeOperation.addBatch(Maps.asMap(keyColumn, (Object) updateValues));
			}
			updatedRowCounter = jdbcBatchingIterator.updatedRowCount;
		}
		// remaining block treatment
		if (lastBlock.size() > 0) {
			deleteStatement = dmlGenerator.buildMassiveDelete(targetTable, keyColumn, lastBlock.size());
			writeOperation = newWriteOperation(deleteStatement, connectionProvider);
			writeOperation.setValue(keyColumn, lastBlock);
			int updatedRowCount = writeOperation.execute();
			updatedRowCounter += updatedRowCount;
		}
		return updatedRowCounter;
	}
	
	private <C> WriteOperation<C> newWriteOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new WriteOperation<>(statement, connectionProvider, writeOperationRetryer);
	}
	
	public List<T> select(Iterable<Serializable> ids) {
		ConnectionProvider connectionProvider = new ConnectionProvider();
		List<T> toReturn = new ArrayList<>(50);
		int blockSize = this.inOperatorMaxSize;
		List<List<Serializable>> parcels = Collections.parcel(ids, blockSize);
		List<Serializable> lastBlock = Iterables.last(parcels);
		PreparedSelect selectStatement;
		ReadOperation<Column> readOperation;
		Table targetTable = mappingStrategy.getTargetTable();
		Set<Column> columnsToRead = targetTable.getColumns().asSet();
		if (parcels.size() > 1) {
			selectStatement = dmlGenerator.buildMassiveSelect(targetTable, columnsToRead, mappingStrategy.getSingleColumnKey(), blockSize);
			if (lastBlock.size() != blockSize) {
				parcels = parcels.subList(0, parcels.size() - 1);
			}
			readOperation = newReadOperation(selectStatement, connectionProvider);
			for (List<Serializable> parcel : parcels) {
				toReturn.addAll(execute(readOperation, mappingStrategy.getSingleColumnKey(), parcel));
			}
		}
		if (lastBlock.size() > 0) {
			selectStatement = dmlGenerator.buildMassiveSelect(targetTable, columnsToRead, mappingStrategy.getSingleColumnKey(), lastBlock.size());
			readOperation = newReadOperation(selectStatement, connectionProvider);
			toReturn.addAll(execute(readOperation, mappingStrategy.getSingleColumnKey(), lastBlock));
		}
		return toReturn;
	}
	
	private <C> ReadOperation<C> newReadOperation(SQLStatement<C> statement, ConnectionProvider connectionProvider) {
		return new ReadOperation<>(statement, connectionProvider);
	}
	
	protected List<T> execute(ReadOperation<Column> operation, Column column, Collection<Serializable> values) {
		List<T> toReturn = new ArrayList<>(values.size());
		try(ReadOperation<Column> closeableOperation = operation) {
			operation.setValue(column, values);
			ResultSet resultSet = closeableOperation.execute();
			RowIterator rowIterator = new RowIterator(resultSet, ((PreparedSelect) closeableOperation.getSqlStatement()).getSelectParameterBinders());
			while (rowIterator.hasNext()) {
				toReturn.add(mappingStrategy.transform(rowIterator.next()));
			}
		} catch (Exception e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return toReturn;
	}
	
	/**
	 * Iterator that triggers batch execution every batch size step.
	 * Usefull for insert and delete statements.
	 */
	private static class JDBCBatchingIterator<E> extends SteppingIterator<E> {
		private final WriteOperation writeOperation;
		private int updatedRowCount;
		
		public JDBCBatchingIterator(Iterable<E> iterable, WriteOperation writeOperation, int batchSize) {
			super(iterable, batchSize);
			this.writeOperation = writeOperation;
		}
		
		@Override
		protected void onStep() {
			this.updatedRowCount += writeOperation.executeBatch();
		}
		
		public int getUpdatedRowCount() {
			return this.updatedRowCount;
		}
	}
	
	/**
	 * Facility to trigger JDBC Batch when number of setted values is reached. Usefull for update statements.
	 * Its principle is near to JDBCBatchingIterator but update methods have to compute differences on each couple so
	 * they generate multiple statements according to differences, hence an Iterator is not a good candidate for design.
	 */
	private static class JDBCBatchingOperation {
		private final WriteOperation<UpwhereColumn> writeOperation;
		private final int batchSize;
		private long stepCounter = 0;
		private int updatedRowCount;
		
		private JDBCBatchingOperation(WriteOperation<UpwhereColumn> writeOperation, int batchSize) {
			this.writeOperation = writeOperation;
			this.batchSize = batchSize;
		}
		
		private void setValues(Map<UpwhereColumn, Object> values) {
			this.writeOperation.addBatch(values);
			this.stepCounter++;
			executeBatchIfNecessary();
		}
		
		private void executeBatchIfNecessary() {
			if (stepCounter == batchSize) {
				executeBatch();
				stepCounter = 0;
			}
		}
		
		private void executeBatch() {
			this.updatedRowCount += writeOperation.executeBatch();
		}
		
		public int getUpdatedRowCount() {
			return updatedRowCount;
		}
	}
	
	/**
	 * Littel classe to mutualize code of {@link #updatePartially(Iterable)} and {@link #updateFully(Iterable)}.
	 */
	private class Updater {
		
		private final IJDBCBatchingOperationProvider batchingOperationProvider;
		private final boolean allColumns;
		
		Updater(IJDBCBatchingOperationProvider batchingOperationProvider, boolean allColumns) {
			this.batchingOperationProvider = batchingOperationProvider;
			this.allColumns = allColumns;
		}
		
		private int update(Iterable<Entry<T, T>> differencesIterable) {
			// building UpdateOperations and update values
			for (Entry<T, T> next : differencesIterable) {
				T modified = next.getKey();
				T unmodified = next.getValue();
				// finding differences between modified instances and unmodified ones
				Map<UpwhereColumn, Object> updateValues = mappingStrategy.getUpdateValues(modified, unmodified, allColumns);
				if (!updateValues.isEmpty()) {
					JDBCBatchingOperation writeOperation = batchingOperationProvider.getJdbcBatchingOperation(updateValues.keySet());
					writeOperation.setValues(updateValues);
				} // else nothing to do (no modification)
			}
			// treating remaining values not yet executed
			int updatedRowCount = 0;
			for (JDBCBatchingOperation jdbcBatchingOperation : batchingOperationProvider.getJdbcBatchingOperations()) {
				if (jdbcBatchingOperation.stepCounter != 0) {
					jdbcBatchingOperation.executeBatch();
				}
				updatedRowCount += jdbcBatchingOperation.getUpdatedRowCount();
			}
			return updatedRowCount;
		}
		
	}
	
	private static interface IJDBCBatchingOperationProvider {
		JDBCBatchingOperation getJdbcBatchingOperation(Set<UpwhereColumn> upwhereColumns);
		Iterable<JDBCBatchingOperation> getJdbcBatchingOperations();
	}
	
	private class MonoJDBCBatchingOperation implements IJDBCBatchingOperationProvider {
		
		private final JDBCBatchingOperation jdbcBatchingOperation;
		
		private MonoJDBCBatchingOperation(JDBCBatchingOperation jdbcBatchingOperation) {
			this.jdbcBatchingOperation = jdbcBatchingOperation;
		}
		
		@Override
		public JDBCBatchingOperation getJdbcBatchingOperation(Set<UpwhereColumn> upwhereColumns) {
			return this.jdbcBatchingOperation;
		}
		
		@Override
		public Iterable<JDBCBatchingOperation> getJdbcBatchingOperations() {
			return Arrays.asList(this.jdbcBatchingOperation);
		}
	}
	
	private class JDBCBatchingOperationCache implements IJDBCBatchingOperationProvider {
		
		private final Map<Set<UpwhereColumn>, JDBCBatchingOperation> updateOperationCache;
		
		private JDBCBatchingOperationCache(final ConnectionProvider connectionProvider) {
			// cache for WriteOperation instances (key is Columns to be updated) for batch use
			updateOperationCache = new ValueFactoryHashMap<>(new IFactory<Set<UpwhereColumn>, JDBCBatchingOperation>() {
				@Override
				public JDBCBatchingOperation createInstance(Set<UpwhereColumn> input) {
					PreparedUpdate preparedUpdate = dmlGenerator.buildUpdate(UpwhereColumn.getUpdateColumns(input), mappingStrategy.getVersionedKeys());
					return new JDBCBatchingOperation(newWriteOperation(preparedUpdate, connectionProvider), PersisterExecutor.this.batchSize);
				}
			});
		}
		
		@Override
		public JDBCBatchingOperation getJdbcBatchingOperation(Set<UpwhereColumn> upwhereColumns) {
			return updateOperationCache.get(upwhereColumns);
		}
		
		@Override
		public Iterable<JDBCBatchingOperation> getJdbcBatchingOperations() {
			return updateOperationCache.values();
		}
	}
	
	/**
	 * Implementation based on TransactionManager.getCurrentConnection()
	 */
	protected class ConnectionProvider implements IConnectionProvider {
		
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
