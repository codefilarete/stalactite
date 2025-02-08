package org.codefilarete.stalactite.engine.runtime;

import java.sql.Savepoint;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.stalactite.sql.RollbackObserver;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.ColumnParameterizedSQL;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement.BindingException;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory.ExpectedBatchedRowCountsSupplier;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * Dedicated class to insert statement execution
 *
 * @param <C> entity type
 * @param <I> identifier type
 * @param <T> table type
 * @author Guillaume Mary
 */
public class InsertExecutor<C, I, T extends Table<T>> extends WriteExecutor<C, I, T> implements org.codefilarete.stalactite.engine.InsertExecutor<C> {
	
	/** Entity lock manager, default is no operation as soon as a {@link VersioningStrategy} is given */
	private OptimisticLockManager<C, T> optimisticLockManager = (OptimisticLockManager<C, T>) OptimisticLockManager.NOOP_OPTIMISTIC_LOCK_MANAGER;
	
	private final IdentifierInsertionManager<C, I> identifierInsertionManager;
	
	private SQLOperationListener<Column<T, ?>> operationListener;
	
	public InsertExecutor(EntityMapping<C, I, T> mappingStrategy, ConnectionConfiguration connectionConfiguration,
						  DMLGenerator dmlGenerator, WriteOperationFactory writeOperationFactory,
						  int inOperatorMaxSize) {
		super(mappingStrategy, connectionConfiguration, dmlGenerator, writeOperationFactory, inOperatorMaxSize);
		this.identifierInsertionManager = mappingStrategy.getIdMapping().getIdentifierInsertionManager();
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
	
	public void setOperationListener(SQLOperationListener<Column<T, ?>> listener) {
		this.operationListener = listener;
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		Set<Column<T, ?>> columns = getMapping().getInsertableColumns();
		ColumnParameterizedSQL<T> insertStatement = getDmlGenerator().buildInsert(columns);
		List<? extends C> entitiesCopy = Iterables.copy(entities);
		ExpectedBatchedRowCountsSupplier expectedBatchedRowCountsSupplier = new ExpectedBatchedRowCountsSupplier(entitiesCopy.size(), getBatchSize());
		
		WriteOperation<Column<T, ?>> writeOperation = getWriteOperationFactory()
				.createInstanceForInsertion(insertStatement, getConnectionProvider(), expectedBatchedRowCountsSupplier);
		writeOperation.setListener(this.operationListener);
		JDBCBatchingIterator<C> jdbcBatchingIterator = identifierInsertionManager.buildJDBCBatchingIterator(entitiesCopy, writeOperation, getBatchSize());
		
		jdbcBatchingIterator.forEachRemaining(entity -> {
			try {
				addToBatch(entity, writeOperation);
			} catch (RuntimeException e) {
				throw new RuntimeException("Error while inserting values for " + entity + " in statement \"" + writeOperation.getSqlStatement().getSQL() + "\"", e);
			}
		});
	}
	
	private void addToBatch(C entity, WriteOperation<Column<T, ?>> writeOperation) {
		Map<Column<T, ?>, ? super Object> insertValues = getMapping().getInsertValues(entity);
		assertMandatoryColumnsHaveNonNullValues(insertValues);
		optimisticLockManager.manageLock(entity, (Map) insertValues);
		writeOperation.addBatch(insertValues);
	}
	
	private void assertMandatoryColumnsHaveNonNullValues(Map<Column<T, ?>, ?> insertValues) {
		Set<Column> nonNullColumnsWithNullValues = Iterables.collect(insertValues.entrySet(),
				e -> !e.getKey().isNullable() && e.getValue() == null, Entry::getKey, HashSet::new);
		if (!nonNullColumnsWithNullValues.isEmpty()) {
			throw new BindingException("Expected non null value for : "
					// we sort result only to stabilize message for tests assertion, do not get it as a business rule
					+ new StringAppender().ccat(Arrays.asTreeSet(Comparator.comparing(Column::getAbsoluteName), nonNullColumnsWithNullValues) , ", "));
		}
	}
	
	/**
	 * The contract for managing Optimistic Lock on insert.
	 * @param <E> entity type
	 * @param <T> table type
	 */
	public interface OptimisticLockManager<E, T extends Table<T>> {
		
		OptimisticLockManager<?, ?> NOOP_OPTIMISTIC_LOCK_MANAGER = (OptimisticLockManager) (o, m) -> {};
		
		/**
		 * Expected to "manage" the optimistic lock:
		 * - can manage it thanks to a versioning column, then must upgrade the entity and takes connection rollback into account
		 * - can manage it by adding modified columns in the where clause
		 *
		 * @param instance
		 * @param updateValues
		 */
		// Note that generics syntax is made for write-only into the Map
		void manageLock(E instance, Map<? super Column<T, ?>, ? super Object> updateValues);
	}
	
	/**
	 * {@link OptimisticLockManager} that sets version value on entity and SQL order
	 * @param <V> version value type
	 * @author Guillaume Mary
	 */
	private class RevertOnRollbackMVCC<V> extends AbstractRevertOnRollbackMVCC<C, V, T> implements OptimisticLockManager<C, T> {
		
		/**
		 * Main constructor.
		 *
		 * @param versioningStrategy the entities upgrader
		 * @param versionColumn the column that stores the version
		 * @param rollbackObserver the {@link RollbackObserver} to revert upgrade when rollback happens
		 * {@link ConnectionProvider#giveConnection()} is not used here, simple mark to help understanding
		 */
		private RevertOnRollbackMVCC(VersioningStrategy<C, V> versioningStrategy, Column<T, V> versionColumn, RollbackObserver rollbackObserver) {
			super(versioningStrategy, versionColumn, rollbackObserver);
		}
		
		/**
		 * Upgrade inserted instance
		 */
		@Override
		public void manageLock(C instance, Map<? super Column<T, ?>, ? super Object> updateValues) {
			V previousVersion = versioningStrategy.getVersion(instance);
			this.versioningStrategy.upgrade(instance);
			V newVersion = versioningStrategy.getVersion(instance);
			updateValues.put(versionColumn, newVersion);
			rollbackObserver.addRollbackListener(new VersioningStrategyRollbackListener<>(versioningStrategy, instance, previousVersion));
		}
	}
	
	/**
	 * {@link RollbackListener} that reverts version upgrade on transaction rollback
	 * @param <C> entity type
	 * @param <V> version value type
	 */
	static class VersioningStrategyRollbackListener<C, V> implements RollbackListener {
		private final VersioningStrategy<C, V> versioningStrategy;
		private final C instance;
		private final V previousVersion;
		
		public VersioningStrategyRollbackListener(VersioningStrategy<C, V> versioningStrategy, C instance, V previousVersion) {
			this.versioningStrategy = versioningStrategy;
			this.instance = instance;
			this.previousVersion = previousVersion;
		}
		
		@Override
		public void beforeRollback() {
			// no pre rollback treatment to do
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

