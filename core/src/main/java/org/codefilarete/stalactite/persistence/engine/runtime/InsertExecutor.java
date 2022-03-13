package org.codefilarete.stalactite.persistence.engine.runtime;

import java.sql.Savepoint;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.persistence.engine.VersioningStrategy;
import org.codefilarete.stalactite.persistence.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.persistence.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.persistence.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory.ExpectedBatchedRowCountsSupplier;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * Dedicated class to insert statement execution
 *
 * @author Guillaume Mary
 */
public class InsertExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> implements org.codefilarete.stalactite.persistence.engine.InsertExecutor<C> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager optimisticLockManager = OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private SQLOperationListener<Column<T, Object>> operationListener;
	
	public InsertExecutor(EntityMappingStrategy<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
		this.identifierInsertionManager = mappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager();
	}
	
	public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
		// we could have put the column as an attribute of the VersioningStrategy but, by making the column more dynamic, the strategy can be
		// shared as long as PropertyAccessor is reusable over entities (wraps a common method)
		Column versionColumn = getMappingStrategy().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
		setOptimisticLockManager(new RevertOnRollbackMVCC(versioningStrategy, versionColumn, getConnectionProvider()));
	}
	
	public void setOptimisticLockManager(OptimisticLockManager optimisticLockManager) {
		this.optimisticLockManager = optimisticLockManager;
	}
	
	public void setOperationListener(SQLOperationListener<Column<T, Object>> listener) {
		this.operationListener = listener;
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		Set<Column<T, Object>> columns = getMappingStrategy().getInsertableColumns();
		ColumnParameterizedSQL<T> insertStatement = getDmlGenerator().buildInsert(columns);
		List<? extends C> entitiesCopy = Iterables.copy(entities);
		ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(entitiesCopy.size(), getBatchSize());
		
		WriteOperation<Column<T, Object>> writeOperation = getWriteOperationFactory()
				.createInstance(insertStatement, getConnectionProvider(), identifierInsertionManager::prepareStatement, expectedBatchedRowCountsSupplier);
		writeOperation.setListener(this.operationListener);
		JDBCBatchingIterator<C> jdbcBatchingIterator = identifierInsertionManager.buildJDBCBatchingIterator(entitiesCopy, writeOperation, getBatchSize());
		
		jdbcBatchingIterator.forEachRemaining(entity -> {
			try {
				addToBatch(entity, writeOperation);
			} catch (RuntimeException e) {
				throw new RuntimeException("Error while inserting values for " + entity, e);
			}
		});
	}
	
	private void addToBatch(C entity, WriteOperation<Column<T, Object>> writeOperation) {
		Map<Column<T, Object>, Object> insertValues = getMappingStrategy().getInsertValues(entity);
		assertMandatoryColumnsHaveNonNullValues(insertValues);
		optimisticLockManager.manageLock(entity, insertValues);
		writeOperation.addBatch(insertValues);
	}
	
	private void assertMandatoryColumnsHaveNonNullValues(Map<Column<T, Object>, Object> insertValues) {
		Set<Column> nonNullColumnsWithNullValues = Iterables.collect(insertValues.entrySet(),
				e -> !e.getKey().isNullable() && e.getValue() == null, Entry::getKey, HashSet::new);
		if (!nonNullColumnsWithNullValues.isEmpty()) {
			throw new BindingException("Expected non null value for : " + new StringAppender().ccat(nonNullColumnsWithNullValues, ", "));
		}
	}
	
	/**
	 * The contract for managing Optimistic Lock on insert.
	 */
	interface OptimisticLockManager<T> {
		
		OptimisticLockManager NOOP_OPTIMISTIC_LOCK_MANAGER = (o, m) -> {};
		
		/**
		 * Expected to "manage" the optimistic lock:
		 * - can manage it thanks to a versioning column, then must upgrade the entity and takes connection rollback into account
		 * - can manage it by adding modified columns in the where clause
		 *
		 * @param instance
		 * @param updateValues
		 */
		void manageLock(T instance, Map<Column, Object> updateValues);
	}
	
	private class RevertOnRollbackMVCC extends AbstractRevertOnRollbackMVCC implements OptimisticLockManager<C> {
		
		/**
		 * Main constructor.
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
		 * @param <C> a {@link ConnectionProvider} that notifies rollback.
		 * {@link ConnectionProvider#giveConnection()} is not used here, simple mark to help understanding
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
		 * Upgrade inserted instance
		 */
		@Override
		public void manageLock(C instance, Map<Column, Object> updateValues) {
			Object previousVersion = versioningStrategy.getVersion(instance);
			this.versioningStrategy.upgrade(instance);
			Object newVersion = versioningStrategy.getVersion(instance);
			updateValues.put(versionColumn, newVersion);
			rollbackObserver.addRollbackListener(new VersioningStrategyRollbackListener<>(versioningStrategy, instance, previousVersion));
		}
	}
	
	/**
	 * 
	 * @param <C>
	 */
	static class VersioningStrategyRollbackListener<C> implements RollbackListener {
		private final VersioningStrategy<C, Object> versioningStrategy;
		private final C instance;
		private final Object previousVersion;
		
		public VersioningStrategyRollbackListener(VersioningStrategy<C, Object> versioningStrategy, C instance, Object previousVersion) {
			this.versioningStrategy = versioningStrategy;
			this.instance = instance;
			this.previousVersion = previousVersion;
		}
		
		@Override
		public void beforeRollback() {
			// no pre rollabck treatment to do
		}
		
		@Override
		public void afterRollback() {
			// We revert the upgrade
			versioningStrategy.revert(instance, previousVersion);
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
	}
	
}

