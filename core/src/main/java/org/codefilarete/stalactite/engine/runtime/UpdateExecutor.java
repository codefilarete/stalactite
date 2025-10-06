package org.codefilarete.stalactite.engine.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener.UpdatePayload;
import org.codefilarete.stalactite.engine.runtime.InsertExecutor.VersioningStrategyRollbackListener;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedUpdate;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory.ExpectedBatchedRowCountsSupplier;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.ArrayIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Predicates;

/**
 * Class dedicated to update statement execution
 * 
 * @param <C> entity type
 * @param <I> identifier type
 * @param <T> table type
 * @author Guillaume Mary
 */
public class UpdateExecutor<C, I, T extends Table<T>> extends WriteExecutor<C, I, T> implements org.codefilarete.stalactite.engine.UpdateExecutor<C> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager<C, T> optimisticLockManager = (OptimisticLockManager<C, T>) OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private SQLOperationListener<UpwhereColumn<T>> operationListener;
	
	public UpdateExecutor(EntityMapping<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
	}
	
	public <V> void setVersioningStrategy(VersioningStrategy<C, V> versioningStrategy) {
		if (!(getConnectionProvider() instanceof RollbackObserver)) {
			throw new UnsupportedOperationException("Version control is only supported for " + Reflections.toString(ConnectionProvider.class)
					+ " that implements " + Reflections.toString(RollbackObserver.class));
		}
		// we could have put the column as an attribute of the VersioningStrategy but, by making the column more dynamic, the strategy can be
		// shared as long as PropertyAccessor is reusable over entities (wraps a common method)
		Column<T, V> versionColumn = (Column<T, V>) getMapping().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
		setOptimisticLockManager(new RevertOnRollbackMVCC<>(versioningStrategy, versionColumn, (RollbackObserver) getConnectionProvider()));
	}
	
	public void setOptimisticLockManager(OptimisticLockManager<C, T> optimisticLockManager) {
		this.optimisticLockManager = optimisticLockManager;
	}
	
	public void setOperationListener(SQLOperationListener<UpwhereColumn<T>> listener) {
		this.operationListener = listener;
	}
	
	private WriteOperation<UpwhereColumn<T>> newWriteOperation(SQLStatement<UpwhereColumn<T>> statement, ConnectionProvider currentConnectionProvider, LongSupplier expectedRowCount) {
		WriteOperation<UpwhereColumn<T>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
		writeOperation.setListener(this.operationListener);
		return writeOperation;
	}
	
	private WriteOperation<UpwhereColumn<T>> newWriteOperation(SQLStatement<UpwhereColumn<T>> statement, ConnectionProvider currentConnectionProvider, long expectedRowCount) {
		WriteOperation<UpwhereColumn<T>> writeOperation = getWriteOperationFactory().createInstance(statement, currentConnectionProvider, expectedRowCount);
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
	public void updateById(Iterable<? extends C> entities) {
		Set<Column<T, ?>> columnsToUpdate = getMapping().getUpdatableColumns();
		if (!columnsToUpdate.isEmpty()) {
			PreparedUpdate<T> updateOperation = getDmlGenerator().buildUpdate(columnsToUpdate, getMapping().getVersionedKeys());
			List<C> entitiesCopy = Iterables.copy(entities);
			ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(entitiesCopy.size(), getBatchSize());
			WriteOperation<UpwhereColumn<T>> writeOperation = newWriteOperation(updateOperation, getConnectionProvider(), expectedBatchedRowCountsSupplier);
			
			JDBCBatchingIterator<C> jdbcBatchingIterator = new JDBCBatchingIterator<>(entities, writeOperation, getBatchSize());
			while (jdbcBatchingIterator.hasNext()) {
				C c = jdbcBatchingIterator.next();
				Map<UpwhereColumn<T>, ?> updateValues = getMapping().getUpdateValues(c, null, true);
				writeOperation.addBatch(updateValues);
			}
		}
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		Iterable<UpdatePayload<C, T>> updatePayloads = computePayloads(differencesIterable, allColumnsStatement);
		if (!Iterables.isEmpty(updatePayloads)) {
			updateDifferences(updatePayloads, allColumnsStatement);
		}
	}
	
	/**
	 * Computes entities payload
	 *
	 * @param differencesIterable entities to persist
	 * @return persistence payloads of entities
	 */
	protected Iterable<UpdatePayload<C, T>> computePayloads(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		return UpdateListener.computePayloads(differencesIterable, allColumnsStatement, getMapping());
	}
	
	// can't be named "update" due to naming conflict with the one with Iterable<Duo>
	public void updateDifferences(Iterable<UpdatePayload<C, T>> updatePayloads, boolean allColumnsStatement) {
		if (allColumnsStatement) {
			updateMappedColumns(updatePayloads);
		} else {
			updateVariousColumns(updatePayloads);
		}
	}
	
	/**
	 * Executes update of given payloads. This method will create as many SQL orders as necessary for them (some cache is used for payload with
	 * same columns to be updated).
	 * This method should be used for heterogeneous use case where payloads don't contain same columns for update.
	 * 
	 * Prefers {@link #updateMappedColumns(Iterable)} if you know that all payloads target the same columns.  
	 * 
	 * This method applies JDBC batch.
	 * 
	 * @param updatePayloads data for SQL order
	 * @see #updateMappedColumns(Iterable) 
	 */
	public void updateVariousColumns(Iterable<? extends UpdatePayload<C, T>> updatePayloads) {
		// we update only entities that have values to be modified
		List<UpdatePayload<C, T>> toUpdate = collectAndAssertNonNullValues(ReadOnlyIterator.wrap(updatePayloads));
		if (!Iterables.isEmpty(toUpdate)) {
			executeUpdate(toUpdate, new JDBCBatchingOperationCache(getConnectionProvider(), toUpdate.size()));
		}
	}
	
	/**
	 * Executes update of given payloads. This method expects that every payload wants to update same columns
	 * as those given by {@link DefaultEntityMapping#getUpdatableColumns()} : this means all mapped columns.
	 * If such a contract is not fulfilled, an exception may occur (because of missing data)
	 * 
	 * This method applies JDBC batch.
	 *
	 * @param updatePayloads data for SQL order
	 * @see #updateVariousColumns(Iterable)
	 */
	public void updateMappedColumns(Iterable<UpdatePayload<C, T>> updatePayloads) {
		// we ask the strategy to lookup for updatable columns (not taken directly on mapping strategy target table)
		Set<Column<T, ?>> columnsToUpdate = getMapping().getUpdatableColumns();
		if (!columnsToUpdate.isEmpty()) {	// we don't execute code below with empty columns to avoid a NPE in buildUpdate(..) due to lack of any element
			// we update only entities that have values to be modified
			List<UpdatePayload<C, T>> toUpdate = collectAndAssertNonNullValues(ReadOnlyIterator.wrap(updatePayloads));
			if (!Iterables.isEmpty(toUpdate)) {
				PreparedUpdate<T> preparedUpdate = getDmlGenerator().buildUpdate(columnsToUpdate, getMapping().getVersionedKeys());
				ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(toUpdate.size(), getBatchSize());
				WriteOperation<UpwhereColumn<T>> writeOperation = newWriteOperation(preparedUpdate, getConnectionProvider(), expectedBatchedRowCountsSupplier);
				// Since all columns are updated we can benefit from JDBC batch
				JDBCBatchingOperation<T> jdbcBatchingOperation = new JDBCBatchingOperation<>(writeOperation, getBatchSize());
				executeUpdate(toUpdate, new SingleJDBCBatchingOperation(jdbcBatchingOperation));
			}
		}
	}
	
	private void executeUpdate(Iterable<UpdatePayload<C, T>> entitiesPayloads, JDBCBatchingOperationProvider<T> batchingOperationProvider) {
		try {
			DifferenceUpdater differenceUpdater = new DifferenceUpdater(batchingOperationProvider);
			differenceUpdater.update(entitiesPayloads);
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
	private List<UpdatePayload<C, T>> collectAndAssertNonNullValues(ReadOnlyIterator<? extends UpdatePayload<C, T>> updatePayloads) {
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
	 * Facility to trigger JDBC Batch when a threshold of {@link #setValues(Map)} count is reached. Useful for update statements.
	 * Its principle is near {@link org.codefilarete.stalactite.engine.runtime.WriteExecutor.JDBCBatchingIterator} but update methods have to compute
	 * differences on each couple therefore it generates multiple statements according to differences, hence an {@link Iterator} is not a good
	 * candidate for current class design.
	 */
	private static class JDBCBatchingOperation<T extends Table<T>> {
		private final WriteOperation<UpwhereColumn<T>> writeOperation;
		private final int batchSize;
		private long stepCounter = 0;
		
		private JDBCBatchingOperation(WriteOperation<UpwhereColumn<T>> writeOperation, int batchSize) {
			this.writeOperation = writeOperation;
			this.batchSize = batchSize;
		}
		
		private void setValues(Map<UpwhereColumn<T>, ?> values) {
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
			writeOperation.executeBatch();
		}
	}
	
	private interface JDBCBatchingOperationProvider<T extends Table<T>> {
		JDBCBatchingOperation<T> getJdbcBatchingOperation(Set<UpwhereColumn<T>> upwhereColumns);
		Iterable<JDBCBatchingOperation<T>> getJdbcBatchingOperations();
	}
	
	private class SingleJDBCBatchingOperation implements JDBCBatchingOperationProvider<T>, Iterable<JDBCBatchingOperation<T>> {
		
		private final JDBCBatchingOperation<T> jdbcBatchingOperation;
		
		private final ArrayIterator<JDBCBatchingOperation<T>> operationIterator;
		
		private SingleJDBCBatchingOperation(JDBCBatchingOperation<T> jdbcBatchingOperation) {
			this.jdbcBatchingOperation = jdbcBatchingOperation;
			this.operationIterator = new ArrayIterator<>(this.jdbcBatchingOperation);
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
		private final ConnectionProvider connectionProvider;
		private final int expectedRowCount;
		
		private JDBCBatchingOperationCache(ConnectionProvider connectionProvider, int expectedRowCount) {
			this.connectionProvider = connectionProvider;
			this.expectedRowCount = expectedRowCount;
		}
		
		@Override
		public JDBCBatchingOperation<T> getJdbcBatchingOperation(Set<UpwhereColumn<T>> upwhereColumns) {
			return updateOperationCache.computeIfAbsent(upwhereColumns, input -> {
				PreparedUpdate<T> preparedUpdate = getDmlGenerator().buildUpdate(UpwhereColumn.getUpdateColumns(input), getMapping().getVersionedKeys());
				return new JDBCBatchingOperation<>(newWriteOperation(preparedUpdate, connectionProvider, expectedRowCount), getBatchSize());
			});
		}
		
		@Override
		public Iterable<JDBCBatchingOperation<T>> getJdbcBatchingOperations() {
			return updateOperationCache.values();
		}
	}
	
	
	/**
	 * Little class to share code of {@link #updateVariousColumns(Iterable)} and {@link #updateMappedColumns(Iterable)}.
	 */
	private class DifferenceUpdater {
		
		private final JDBCBatchingOperationProvider<T> batchingOperationProvider;
		
		DifferenceUpdater(JDBCBatchingOperationProvider<T> batchingOperationProvider) {
			this.batchingOperationProvider = batchingOperationProvider;
		}
		
		private void update(Iterable<UpdatePayload<C, T>> toUpdate) {
			// building UpdateOperations and update values
			toUpdate.forEach(p -> {
				Map<UpwhereColumn<T>, ?> updateValues = p.getValues();
				optimisticLockManager.manageLock(p.getEntities().getLeft(), p.getEntities().getRight(), (Map<UpwhereColumn<T>, Object>) updateValues);
				JDBCBatchingOperation<T> writeOperation = batchingOperationProvider.getJdbcBatchingOperation(updateValues.keySet());
				writeOperation.setValues(updateValues);
			});
			// treating remaining values not yet executed
			for (JDBCBatchingOperation jdbcBatchingOperation : batchingOperationProvider.getJdbcBatchingOperations()) {
				if (jdbcBatchingOperation.stepCounter != 0) {
					jdbcBatchingOperation.executeBatch();
				}
			}
		}
	}
	
	/**
	 * The contract for managing Optimistic Lock on update.
	 * @param <E> entity type
	 * @param <T> table type
	 */
	public interface OptimisticLockManager<E, T extends Table<T>> {
		
		OptimisticLockManager<?, ?> NOOP_OPTIMISTIC_LOCK_MANAGER = (OptimisticLockManager) (o1, o2, m) -> {};
		
		/**
		 * Expected to "manage" the optimistic lock:
		 * - can manage it thanks to a versioning column, then must upgrade the entity and takes connection rollback into account
		 * - can manage it by adding modified columns in the where clause
		 * 
		 * @param modified
		 * @param unmodified
		 * @param updateValues
		 */
		// Note that generics syntax is made for write-only into the Map
		void manageLock(E modified, E unmodified, Map<UpwhereColumn<T>, Object> updateValues);
	}
	
	private class RevertOnRollbackMVCC<E, V> extends AbstractRevertOnRollbackMVCC<E, V, T> implements OptimisticLockManager<E, T> {
		
		/**
		 * Main constructor.
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
		 * {@link ConnectionProvider#giveConnection()} is not used here, simple mark to help understanding
		 */
		private RevertOnRollbackMVCC(VersioningStrategy<E, V> versioningStrategy, Column<T, V> versionColumn, RollbackObserver rollbackObserver) {
			super(versioningStrategy, versionColumn, rollbackObserver);
		}
		
		/**
		 * Upgrades modified instance and adds version column to the update statement through {@link UpwhereColumn}s
		 */
		@Override
		public void manageLock(E modified, E unmodified, Map<UpwhereColumn<T>, Object> updateValues) {
			V modifiedVersion = versioningStrategy.getVersion(modified);
			V unmodifiedVersion = versioningStrategy.getVersion(unmodified);
			if (!Predicates.equalOrNull(modifiedVersion, modifiedVersion)) {
				throw new IllegalStateException();
			}
			versioningStrategy.upgrade(modified);
			updateValues.put(new UpwhereColumn<>(versionColumn, true), versioningStrategy.getVersion(modified));
			updateValues.put(new UpwhereColumn<>(versionColumn, false), unmodifiedVersion);
			rollbackObserver.addRollbackListener(new VersioningStrategyRollbackListener<>(versioningStrategy, modified, modifiedVersion));
		}
	}
}
