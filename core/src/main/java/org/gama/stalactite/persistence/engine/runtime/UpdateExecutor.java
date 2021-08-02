package org.gama.stalactite.persistence.engine.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.Retryer;
import org.gama.lang.collection.ArrayIterator;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.lang.function.Predicates;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.RowCountManager;
import org.gama.stalactite.persistence.engine.RowCountManager.RowCounter;
import org.gama.stalactite.persistence.engine.VersioningStrategy;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.engine.runtime.InsertExecutor.VersioningStrategyRollbackListener;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.RollbackObserver;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

import static org.gama.stalactite.persistence.engine.RowCountManager.THROWING_ROW_COUNT_MANAGER;

/**
 * Class dedicated to update statement execution
 * 
 * @author Guillaume Mary
 */
public class UpdateExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> implements IUpdateExecutor<C> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager<T> optimisticLockManager = OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private RowCountManager rowCountManager = RowCountManager.THROWING_ROW_COUNT_MANAGER;
	
	private SQLOperationListener<UpwhereColumn<T>> operationListener;
	
	public UpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, IConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationRetryer, inOperatorMaxSize);
	}
	
	public void setRowCountManager(RowCountManager rowCountManager) {
		this.rowCountManager = rowCountManager;
	}
	
	public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
		// we could have put the column as an attribute of the VersioningStrategy but, by making the column more dynamic, the strategy can be
		// shared as long as PropertyAccessor is reusable over entities (wraps a common method)
		Column<T, Object> versionColumn = getMappingStrategy().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
		setOptimisticLockManager(new RevertOnRollbackMVCC(versioningStrategy, versionColumn, getConnectionProvider()));
		setRowCountManager(THROWING_ROW_COUNT_MANAGER);
	}
	
	public void setOptimisticLockManager(OptimisticLockManager<T> optimisticLockManager) {
		this.optimisticLockManager = optimisticLockManager;
	}
	
	public void setOperationListener(SQLOperationListener<UpwhereColumn<T>> listener) {
		this.operationListener = listener;
	}
	
	private WriteOperation<UpwhereColumn<T>> newWriteOperation(SQLStatement<UpwhereColumn<T>> statement, CurrentConnectionProvider currentConnectionProvider) {
		WriteOperation<UpwhereColumn<T>> writeOperation = new WriteOperation<>(statement, currentConnectionProvider, getWriteOperationRetryer());
		writeOperation.setListener(this.operationListener);
		return writeOperation;
	}
	
	/**
	 * Updates roughly some instances: no difference are computed, only update statements (all columns) are applied.
	 * Hence optimistic lock (versioned entities) is not checked
	 * 
	 * @param entities iterable of entities
	 */
	@Override
	public int updateById(Iterable<C> entities) {
		Set<Column<T, Object>> columnsToUpdate = getMappingStrategy().getUpdatableColumns();
		if (columnsToUpdate.isEmpty()) {
			// nothing to update, this prevent a NPE in buildUpdate due to lack of any (first) element
			return 0;
		} else {
			PreparedUpdate<T> updateOperation = getDmlGenerator().buildUpdate(columnsToUpdate, getMappingStrategy().getVersionedKeys());
			WriteOperation<UpwhereColumn<T>> writeOperation = newWriteOperation(updateOperation, new CurrentConnectionProvider());
			
			JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entities, writeOperation, getBatchSize());
			while (jdbcBatchingIterator.hasNext()) {
				C c = jdbcBatchingIterator.next();
				Map<UpwhereColumn<T>, Object> updateValues = getMappingStrategy().getUpdateValues(c, null, true);
				writeOperation.addBatch(updateValues);
			}
			return jdbcBatchingIterator.getUpdatedRowCount();
		}
	}
	
	@Override
	public int update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		Iterable<UpdatePayload<C, T>> updatePayloads = computePayloads(differencesIterable, allColumnsStatement);
		if (Iterables.isEmpty(updatePayloads)) {
			// nothing to update => we return immediatly without any call to listeners
			return 0;
		} else {
			return updateDifferences(updatePayloads, allColumnsStatement);
		}
		
	}
	
	/**
	 * Computes entities payload
	 *
	 * @param differencesIterable entities to persist
	 * @return persistence payloads of entities
	 */
	protected Iterable<UpdatePayload<C, T>> computePayloads(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		return UpdateListener.computePayloads(differencesIterable, allColumnsStatement, getMappingStrategy());
	}
	
	// can't be named "update" due to naming conflict with the one with Iterable<Duo>
	public int updateDifferences(Iterable<UpdatePayload<C, T>> updatePayloads, boolean allColumnsStatement) {
		if (allColumnsStatement) {
			return updateMappedColumns(updatePayloads);
		} else {
			return updateVariousColumns(updatePayloads);
		}
	}
	
	/**
	 * Executes update of given payloads. This method will create as many SQL orders as necessary for them (some cache is used for payload with
	 * same columns to be updated).
	 * This method should be used for heterogeneous use case where payloads doesn't contain same columns for update.
	 * 
	 * Prefers {@link #updateMappedColumns(Iterable)} if you know that all payloads target the same columns.  
	 * 
	 * This method applies JDBC batch.
	 * 
	 * @param updatePayloads data for SQL order
	 * @see #updateMappedColumns(Iterable) 
	 */
	public int updateVariousColumns(Iterable<UpdatePayload<C, T>> updatePayloads) {
		// we update only entities that have values to be modified
		Iterable<UpdatePayload<C, T>> toUpdate = collectAndAssertNonNullValues(ReadOnlyIterator.wrap(updatePayloads));
		if (!Iterables.isEmpty(toUpdate)) {
			CurrentConnectionProvider currentConnectionProvider = new CurrentConnectionProvider();
			return executeUpdate(toUpdate, new JDBCBatchingOperationCache(currentConnectionProvider));
		} else {
			return 0;
		}
	}
	
	/**
	 * Executes update of given payloads. This method expects that every payload wants to update same columns
	 * as those given by {@link ClassMappingStrategy#getUpdatableColumns()} : this means all mapped columns.
	 * If such a contract is not fullfilled, an exception may occur (because of missing data)
	 * 
	 * This method applies JDBC batch.
	 *
	 * @param updatePayloads data for SQL order
	 * @see #updateVariousColumns(Iterable)
	 */
	public int updateMappedColumns(Iterable<UpdatePayload<C, T>> updatePayloads) {
		// we ask the strategy to lookup for updatable columns (not taken directly on mapping strategy target table)
		Set<Column<T, Object>> columnsToUpdate = getMappingStrategy().getUpdatableColumns();
		if (columnsToUpdate.isEmpty()) {
			// nothing to update, this prevent from a NPE in buildUpdate(..) due to lack of any element
			return 0;
		} else {
			// we update only entities that have values to be modified
			Iterable<UpdatePayload<C, T>> toUpdate = collectAndAssertNonNullValues(ReadOnlyIterator.wrap(updatePayloads));
			if (!Iterables.isEmpty(toUpdate)) {
				PreparedUpdate<T> preparedUpdate = getDmlGenerator().buildUpdate(columnsToUpdate, getMappingStrategy().getVersionedKeys());
				WriteOperation<UpwhereColumn<T>> writeOperation = newWriteOperation(preparedUpdate, new CurrentConnectionProvider());
				// Since all columns are updated we can benefit from JDBC batch
				JDBCBatchingOperation<T> jdbcBatchingOperation = new JDBCBatchingOperation<>(writeOperation, getBatchSize());
				return executeUpdate(toUpdate, new SingleJDBCBatchingOperation(jdbcBatchingOperation));
			} else {
				return 0;
			}
		}
	}
	
	private int executeUpdate(Iterable<UpdatePayload<C, T>> entitiesPayloads, JDBCBatchingOperationProvider<T> batchingOperationProvider) {
		try {
			DifferenceUpdater differenceUpdater = new DifferenceUpdater(batchingOperationProvider);
			return differenceUpdater.update(entitiesPayloads);
		} catch (RuntimeException e) {
			throw new RuntimeException("Error while updating values", e);
		}
	}
	
	/**
	 * ARGUMENT SHOULD NOT BE ALTERED (ensured via readonly mark), else it corrupts callers which may pass this argument to some listeners, then
	 * those ones will lack some data.
	 * 
	 * @param updatePayloads payloads expected to be updated 
	 * @return a copy of the argument, without those which values are empty
	 * @throws IllegalArgumentException
	 */
	private Iterable<UpdatePayload<C, T>> collectAndAssertNonNullValues(ReadOnlyIterator<UpdatePayload<C, T>> updatePayloads) {
		List<UpdatePayload<C, T>> result = new ArrayList<>(getBatchSize());	// we set a list size as a small performance improvement to prevent too many list extend
		updatePayloads.forEachRemaining(payload -> {
			if (!payload.getValues().isEmpty()) {
				payload.getValues().forEach((k, v) -> {
					if (!k.getColumn().isNullable() && v == null) {
						throw new IllegalArgumentException("Expected non null value for column " + k.getColumn()
								// we print the instance roughly, how could we do better ?
								+ " on instance " + payload.getEntities().getLeft());
					}
				});
				result.add(payload);
			}
		});
		return result;
	}
	
	/**
	 * Facility to trigger JDBC Batch when number of setted values is reached. Usefull for update statements.
	 * Its principle is near to JDBCBatchingIterator but update methods have to compute differences on each couple so
	 * they generate multiple statements according to differences, hence an Iterator is not a good candidate for design.
	 */
	private static class JDBCBatchingOperation<T extends Table> {
		private final WriteOperation<UpwhereColumn<T>> writeOperation;
		private final int batchSize;
		private long stepCounter = 0;
		private int updatedRowCount;
		
		private JDBCBatchingOperation(WriteOperation<UpwhereColumn<T>> writeOperation, int batchSize) {
			this.writeOperation = writeOperation;
			this.batchSize = batchSize;
		}
		
		private void setValues(Map<UpwhereColumn<T>, Object> values) {
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
	
	private interface JDBCBatchingOperationProvider<T extends Table> {
		JDBCBatchingOperation<T> getJdbcBatchingOperation(Set<UpwhereColumn<T>> upwhereColumns);
		Iterable<JDBCBatchingOperation<T>> getJdbcBatchingOperations();
	}
	
	private class SingleJDBCBatchingOperation implements JDBCBatchingOperationProvider<T>, Iterable<JDBCBatchingOperation<T>> {
		
		private final JDBCBatchingOperation<T> jdbcBatchingOperation;
		
		private final ArrayIterator<JDBCBatchingOperation<T>> operationIterator;
		
		private SingleJDBCBatchingOperation(JDBCBatchingOperation<T> jdbcBatchingOperation) {
			this.jdbcBatchingOperation = jdbcBatchingOperation;
			operationIterator = new ArrayIterator<>(this.jdbcBatchingOperation);
		}
		
		@Override
		public JDBCBatchingOperation<T> getJdbcBatchingOperation(Set<UpwhereColumn<T>> upwhereColumns) {
			return this.jdbcBatchingOperation;
		}
		
		@Override
		public Iterable<JDBCBatchingOperation<T>> getJdbcBatchingOperations() {
			return this;
		}
		
		@Override
		public Iterator<JDBCBatchingOperation<T>> iterator() {
			return operationIterator;
		}
	}
	
	/**
	 * Batching operation with cache based on SQL statement, more exactly based on Columns to be updated.
	 */
	private class JDBCBatchingOperationCache implements JDBCBatchingOperationProvider<T> {
		
		/** cache for WriteOperation instances (key is Columns to be updated) for batch use */
		private final Map<Set<UpwhereColumn<T>>, JDBCBatchingOperation<T>> updateOperationCache = new HashMap<>();
		private final DMLExecutor<C, I, T>.CurrentConnectionProvider currentConnectionProvider;
		
		private JDBCBatchingOperationCache(CurrentConnectionProvider currentConnectionProvider) {
			this.currentConnectionProvider = currentConnectionProvider;
		}
		
		@Override
		public JDBCBatchingOperation<T> getJdbcBatchingOperation(Set<UpwhereColumn<T>> upwhereColumns) {
			return updateOperationCache.computeIfAbsent(upwhereColumns, input -> {
				PreparedUpdate<T> preparedUpdate = getDmlGenerator().buildUpdate(UpwhereColumn.getUpdateColumns(input), getMappingStrategy().getVersionedKeys());
				return new JDBCBatchingOperation<>(newWriteOperation(preparedUpdate, currentConnectionProvider), getBatchSize());
			});
		}
		
		@Override
		public Iterable<JDBCBatchingOperation<T>> getJdbcBatchingOperations() {
			return updateOperationCache.values();
		}
	}
	
	
	/**
	 * Little class to mutualize code of {@link #updateVariousColumns(Iterable)} and {@link #updateMappedColumns(Iterable)}.
	 */
	private class DifferenceUpdater {
		
		private final JDBCBatchingOperationProvider<T> batchingOperationProvider;
		
		DifferenceUpdater(JDBCBatchingOperationProvider<T> batchingOperationProvider) {
			this.batchingOperationProvider = batchingOperationProvider;
		}
		
		private int update(Iterable<UpdatePayload<C, T>> toUpdate) {
			RowCounter rowCounter = new RowCounter();
			// building UpdateOperations and update values
			toUpdate.forEach(p -> {
				Map<UpwhereColumn<T>, Object> updateValues = p.getValues();
				optimisticLockManager.manageLock(p.getEntities().getLeft(), p.getEntities().getRight(), updateValues);
				JDBCBatchingOperation<T> writeOperation = batchingOperationProvider.getJdbcBatchingOperation(updateValues.keySet());
				writeOperation.setValues(updateValues);
				// we keep the updated values for row count, not glad with it but not found any way to do differently
				rowCounter.add(updateValues);
			});
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
	interface OptimisticLockManager<T extends Table> {
		
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
		void manageLock(Object modified, Object unmodified, Map<UpwhereColumn<T>, Object> updateValues);
	}
	
	private class RevertOnRollbackMVCC extends AbstractRevertOnRollbackMVCC implements OptimisticLockManager<T> {
		
		/**
		 * Main constructor.
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
		 * @param <C> a {@link ConnectionProvider} that notifies rollback.
		 * {@link ConnectionProvider#getCurrentConnection()} is not used here, simple mark to help understanding
		 */
		private <C extends RollbackObserver & ConnectionProvider> RevertOnRollbackMVCC(VersioningStrategy versioningStrategy, Column<T, Object> versionColumn, C rollbackObserver) {
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
		 * Upgrades modified instance and adds version column to the update statement through {@link UpwhereColumn}s
		 */
		@Override
		public void manageLock(Object modified, Object unmodified, Map<UpwhereColumn<T>, Object> updateValues) {
			Object modifiedVersion = versioningStrategy.getVersion(modified);
			Object unmodifiedVersion = versioningStrategy.getVersion(unmodified);
			if (!Predicates.equalOrNull(modifiedVersion, modifiedVersion)) {
				throw new IllegalStateException();
			}
			versioningStrategy.upgrade(modified);
			updateValues.put(new UpwhereColumn<T>(versionColumn, true), versioningStrategy.getVersion(modified));
			updateValues.put(new UpwhereColumn<T>(versionColumn, false), unmodifiedVersion);
			rollbackObserver.addRollbackListener(new VersioningStrategyRollbackListener<>(versioningStrategy, modified, modifiedVersion));
		}
		
	}
}
