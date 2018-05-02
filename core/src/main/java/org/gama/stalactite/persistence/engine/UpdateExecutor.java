package org.gama.stalactite.persistence.engine;

import java.sql.Savepoint;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.ValueFactoryHashMap;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.RollbackListener;
import org.gama.sql.RollbackObserver;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.engine.RowCountManager.RowCounter;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;

import static org.gama.stalactite.persistence.engine.RowCountManager.THROWING_ROW_COUNT_MANAGER;

/**
 * Class dedicated to update statement execution
 * 
 * @author Guillaume Mary
 */
public class UpdateExecutor<T, I> extends UpsertExecutor<T, I> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager optimisticLockManager = OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private RowCountManager rowCountManager = RowCountManager.THROWING_ROW_COUNT_MANAGER;
	
	public UpdateExecutor(ClassMappingStrategy<T, I> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
	}
	
	public void setRowCountManager(RowCountManager rowCountManager) {
		this.rowCountManager = rowCountManager;
	}
	
	public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
		// we could have put the column as an attribute of the VersioningStrategy but, by making the column more dynamic, the strategy can be
		// shared as long as PropertyAccessor is reusable over entities (wraps a common method)
		Column versionColumn = getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(versioningStrategy.getPropertyAccessor());
		setOptimisticLockManager(new RevertOnRollbackMVCC(versioningStrategy, versionColumn, getConnectionProvider()));
		setRowCountManager(THROWING_ROW_COUNT_MANAGER);
	}
	
	public void setOptimisticLockManager(OptimisticLockManager optimisticLockManager) {
		this.optimisticLockManager = optimisticLockManager;
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * Hence optimistic lock (versioned entities) is not check
	 * @param iterable iterable of instances
	 */
	public int updateById(Iterable<T> iterable) {
		Set<Column> columnsToUpdate = getMappingStrategy().getUpdatableColumns();
		PreparedUpdate updateOperation = getDmlGenerator().buildUpdate(columnsToUpdate, getMappingStrategy().getVersionedKeys());
		WriteOperation<PreparedUpdate.UpwhereColumn> writeOperation = newWriteOperation(updateOperation, new CurrentConnectionProvider());
		
		JDBCBatchingIterator<T> jdbcBatchingIterator = new JDBCBatchingIterator<>(iterable, writeOperation, getBatchSize());
		while(jdbcBatchingIterator.hasNext()) {
			T t = jdbcBatchingIterator.next();
			Map<PreparedUpdate.UpwhereColumn, Object> updateValues = getMappingStrategy().getUpdateValues(t, null, true);
			writeOperation.addBatch(updateValues);
		}
		return jdbcBatchingIterator.getUpdatedRowCount();
	}
	
	/**
	 * Update instances that have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 */
	public int update(Iterable<Map.Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
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
	public int updatePartially(Iterable<Map.Entry<T, T>> differencesIterable) {
		CurrentConnectionProvider currentConnectionProvider = new CurrentConnectionProvider();
		return new DifferenceUpdater(new JDBCBatchingOperationCache(currentConnectionProvider), false).update(differencesIterable);
	}
	
	/**
	 * Update instances that have changes. All columns are updated.
	 * Groups statements to benefit from JDBC batch.
	 *
	 * @param differencesIterable iterable of instances
	 */
	public int updateFully(Iterable<Map.Entry<T, T>> differencesIterable) {
		Table targetTable = getMappingStrategy().getTargetTable();
		// we never update primary key (by principle and for persistent bean cache based on id (on what else ?)) 
		Set<Column> columnsToUpdate = targetTable.getColumnsNoPrimaryKey();
		PreparedUpdate preparedUpdate = getDmlGenerator().buildUpdate(columnsToUpdate, getMappingStrategy().getVersionedKeys());
		WriteOperation<PreparedUpdate.UpwhereColumn> writeOperation = newWriteOperation(preparedUpdate, new CurrentConnectionProvider());
		// Since all columns are updated we can benefit from JDBC batch
		JDBCBatchingOperation jdbcBatchingOperation = new JDBCBatchingOperation(writeOperation, getBatchSize());
		
		return new DifferenceUpdater(new SingleJDBCBatchingOperation(jdbcBatchingOperation), true).update(differencesIterable);
	}
	
	/**
	 * Facility to trigger JDBC Batch when number of setted values is reached. Usefull for update statements.
	 * Its principle is near to JDBCBatchingIterator but update methods have to compute differences on each couple so
	 * they generate multiple statements according to differences, hence an Iterator is not a good candidate for design.
	 */
	private static class JDBCBatchingOperation {
		private final WriteOperation<PreparedUpdate.UpwhereColumn> writeOperation;
		private final int batchSize;
		private long stepCounter = 0;
		private int updatedRowCount;
		
		private JDBCBatchingOperation(WriteOperation<PreparedUpdate.UpwhereColumn> writeOperation, int batchSize) {
			this.writeOperation = writeOperation;
			this.batchSize = batchSize;
		}
		
		private void setValues(Map<PreparedUpdate.UpwhereColumn, Object> values) {
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
	
	private interface JDBCBatchingOperationProvider {
		JDBCBatchingOperation getJdbcBatchingOperation(Set<PreparedUpdate.UpwhereColumn> upwhereColumns);
		Iterable<JDBCBatchingOperation> getJdbcBatchingOperations();
	}
	
	private class SingleJDBCBatchingOperation implements JDBCBatchingOperationProvider, Iterable<JDBCBatchingOperation> {
		
		private final JDBCBatchingOperation[] jdbcBatchingOperation = new JDBCBatchingOperation[1];
		
		private final ArrayIterator<JDBCBatchingOperation> operationIterator = new ArrayIterator<>(jdbcBatchingOperation);
		
		private SingleJDBCBatchingOperation(JDBCBatchingOperation jdbcBatchingOperation) {
			this.jdbcBatchingOperation[0] = jdbcBatchingOperation;
		}
		
		@Override
		public JDBCBatchingOperation getJdbcBatchingOperation(Set<PreparedUpdate.UpwhereColumn> upwhereColumns) {
			return this.jdbcBatchingOperation[0];
		}
		
		@Override
		public Iterable<JDBCBatchingOperation> getJdbcBatchingOperations() {
			return this;
		}
		
		@Override
		public Iterator<JDBCBatchingOperation> iterator() {
			return operationIterator;
		}
	}
	
	private class JDBCBatchingOperationCache implements JDBCBatchingOperationProvider {
		
		private final Map<Set<PreparedUpdate.UpwhereColumn>, JDBCBatchingOperation> updateOperationCache;
		
		private JDBCBatchingOperationCache(final CurrentConnectionProvider currentConnectionProvider) {
			// cache for WriteOperation instances (key is Columns to be updated) for batch use
			updateOperationCache = new ValueFactoryHashMap<>(input -> {
				PreparedUpdate preparedUpdate = getDmlGenerator().buildUpdate(UpwhereColumn.getUpdateColumns(input), getMappingStrategy().getVersionedKeys());
				return new JDBCBatchingOperation(newWriteOperation(preparedUpdate, currentConnectionProvider), getBatchSize());
			});
		}
		
		@Override
		public JDBCBatchingOperation getJdbcBatchingOperation(Set<PreparedUpdate.UpwhereColumn> upwhereColumns) {
			return updateOperationCache.get(upwhereColumns);
		}
		
		@Override
		public Iterable<JDBCBatchingOperation> getJdbcBatchingOperations() {
			return updateOperationCache.values();
		}
	}
	
	
	/**
	 * Little class to mutualize code of {@link #updatePartially(Iterable)} and {@link #updateFully(Iterable)}.
	 */
	private class DifferenceUpdater {
		
		private final JDBCBatchingOperationProvider batchingOperationProvider;
		private final boolean allColumns;
		
		DifferenceUpdater(JDBCBatchingOperationProvider batchingOperationProvider, boolean allColumns) {
			this.batchingOperationProvider = batchingOperationProvider;
			this.allColumns = allColumns;
		}
		
		private int update(Iterable<Map.Entry<T, T>> differencesIterable) {
			RowCounter rowCounter = new RowCounter();
			// building UpdateOperations and update values
			for (Map.Entry<T, T> next : differencesIterable) {
				T modified = next.getKey();
				T unmodified = next.getValue();
				// finding differences between modified instances and unmodified ones
				Map<PreparedUpdate.UpwhereColumn, Object> updateValues = getMappingStrategy().getUpdateValues(modified, unmodified, allColumns);
				if (!updateValues.isEmpty()) {
					optimisticLockManager.manageLock(modified, unmodified, updateValues);
					JDBCBatchingOperation writeOperation = batchingOperationProvider.getJdbcBatchingOperation(updateValues.keySet());
					writeOperation.setValues(updateValues);
					// we keep the updated values for row count, not glad with it but not found any way to do differently
					rowCounter.add(updateValues);
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
			rowCountManager.checkRowCount(rowCounter, updatedRowCount);
			return updatedRowCount;
		}
		
	}
	
	/**
	 * The contract for managing Optimistic Lock on update.
	 */
	interface OptimisticLockManager {
		
		OptimisticLockManager NOOP_OPTIMISTIC_LOCK_MANAGER = (o1, o2, m) -> {};
		
		/**
		 * Expected to "manage" the optimistic lock:
		 * - can manage it thanks to a versioning column, then must upgrade the entity and takes connection rollback into account
		 * - can manage it by adding modified columns in the where clause
		 * 
		 * @param modified
		 * @param unmodified
		 * @param updateValues
		 */
		void manageLock(Object modified, Object unmodified, Map<UpwhereColumn, Object> updateValues);
	}
	
	private class RevertOnRollbackMVCC extends AbstractRevertOnRollbackMVCC implements OptimisticLockManager {
		
		/**
		 * Main constructor.
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
		 * @param <C> a {@link ConnectionProvider} that notifies rollback.
		 * {@link ConnectionProvider#getCurrentConnection()} is not used here, simple mark to help understanding
		 */
		private <C extends RollbackObserver & ConnectionProvider> RevertOnRollbackMVCC(VersioningStrategy versioningStrategy, Column versionColumn, C rollbackObserver) {
			super(versioningStrategy, versionColumn, rollbackObserver);
		}
		
		/**
		 * Constructor that will check that the given {@link ConnectionProvider} is also a {@link RollbackObserver}, as the other constructor
		 * expects it. Will throw an {@link UnsupportedOperationException} if it is not the case
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver a {@link ConnectionProvider} that implements {@link RollbackObserver} to revert upgrade when rollback happens
		 * @throws UnsupportedOperationException if the given {@link ConnectionProvider} doesn't implements {@link RollbackObserver}
		 */
		private RevertOnRollbackMVCC(VersioningStrategy versioningStrategy, Column versionColumn, ConnectionProvider rollbackObserver) {
			super(versioningStrategy, versionColumn, rollbackObserver);
		}
		
		/**
		 * Upgrade modified instance and add version column to the update statement through {@link UpwhereColumn}s
		 */
		@Override
		public void manageLock(Object modified, Object unmodified, Map<UpwhereColumn, Object> updateValues) {
			Object modifiedVersion = versioningStrategy.getVersion(modified);
			Object unmodifiedVersion = versioningStrategy.getVersion(unmodified);
			if (!Objects.equalsWithNull(modifiedVersion, modifiedVersion)) {
				throw new IllegalStateException();
			}
			versioningStrategy.upgrade(modified);
			updateValues.put(new UpwhereColumn(versionColumn, true), versioningStrategy.getVersion(modified));
			updateValues.put(new UpwhereColumn(versionColumn, false), unmodifiedVersion);
			rollbackObserver.addRollbackListener(new RollbackListener() {
				@Override
				public void beforeRollback() {
					// no pre rollabck treatment to do
				}
				
				@Override
				public void afterRollback() {
					// We revert the upgrade
					versioningStrategy.revert(modified, modifiedVersion);
				}
				
				@Override
				public void beforeRollback(Savepoint savepoint) {
					// not implemented
				}
				
				@Override
				public void afterRollback(Savepoint savepoint) {
					// not implemented : should we do the same as default rollback ?
					// it depends on if entity versioning was done during this savepoint ... how to know ?
				}
				
				@Override
				public boolean isTemporary() {
					// we don't need this on each rollback
					return true;
				}
			});
		}
		
	}
}
