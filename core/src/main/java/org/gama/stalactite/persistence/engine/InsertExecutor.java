package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;
import java.util.Set;

import org.gama.lang.Retryer;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.RollbackListener;
import org.gama.sql.RollbackObserver;
import org.gama.sql.dml.SQLStatement;
import org.gama.sql.dml.WriteOperation;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Dedicated class to insert statement execution
 *
 * @author Guillaume Mary
 */
public class InsertExecutor<C, I, T extends Table> extends WriteExecutor<C, I, T> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager optimisticLockManager = OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	public InsertExecutor(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
						  DMLGenerator dmlGenerator, Retryer writeOperationRetryer,
						  int batchSize, int inOperatorMaxSize) {
		super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
		this.identifierInsertionManager = mappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager();
	}
	
	public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
		// we could have put the column as an attribute of the VersioningStrategy but, by making the column more dynamic, the strategy can be
		// shared as long as PropertyAccessor is reusable over entities (wraps a common method)
		Column versionColumn = getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(versioningStrategy.getPropertyAccessor());
		setOptimisticLockManager(new RevertOnRollbackMVCC(versioningStrategy, versionColumn, getConnectionProvider()));
	}
	
	public void setOptimisticLockManager(OptimisticLockManager optimisticLockManager) {
		this.optimisticLockManager = optimisticLockManager;
	}
	
	@Override
	protected <P> WriteOperation<P> newWriteOperation(SQLStatement<P> statement, CurrentConnectionProvider currentConnectionProvider) {
		return new WriteOperation<P>(statement, currentConnectionProvider, getWriteOperationRetryer()) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				// NB: simple implementation: we don't use the column-specifying signature since not all databases support reading by column name
				this.preparedStatement = identifierInsertionManager.prepareStatement(connection, getSQL());
			}
		};
	}
	
	public int insert(Iterable<C> entities) {
		Set<Column<T, Object>> columns = getMappingStrategy().getInsertableColumns();
		ColumnParamedSQL<T> insertStatement = getDmlGenerator().buildInsert(columns);
		WriteOperation<Column<T, ?>> writeOperation = newWriteOperation(insertStatement, new CurrentConnectionProvider());
		JDBCBatchingIterator<C> jdbcBatchingIterator = identifierInsertionManager.buildJDBCBatchingIterator(entities, writeOperation, getBatchSize());
		
		jdbcBatchingIterator.forEachRemaining(c -> {
			Map<Column<T, Object>, Object> insertValues = getMappingStrategy().getInsertValues(c);
			optimisticLockManager.manageLock(c, insertValues);
			writeOperation.addBatch((Map) insertValues);
		});
		return jdbcBatchingIterator.getUpdatedRowCount();
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
		 * Upgrade inserted instance
		 */
		@Override
		public void manageLock(C instance, Map<Column, Object> updateValues) {
			Object previousVersion = versioningStrategy.getVersion(instance);
			this.versioningStrategy.upgrade(instance);
			Object newVersion = versioningStrategy.getVersion(instance);
			updateValues.put(versionColumn, newVersion);
			rollbackObserver.addRollbackListener(new RollbackListener() {
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
			});
		}
		
	}
}

