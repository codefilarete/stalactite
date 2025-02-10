package org.codefilarete.stalactite.spring.transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Set;

import org.codefilarete.stalactite.sql.CommitListener;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.RollbackListener;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * A {@link org.springframework.transaction.TransactionManager} appropriate for Stalactite.
 * It creates a bridge between Spring {@link org.springframework.transaction.TransactionManager} and Stalactite
 * {@link org.codefilarete.stalactite.sql.ConnectionProvider} to make the latter uses transactions managed by Spring :
 * the only thing to do is to get the {@link Connection} from {@link TransactionSynchronizationManager}. Rollbacks and
 * commits are managed Spring (Stalactite never manages transaction).
 * <p>
 * It inherits from {@link JdbcTransactionManager} because Stalactite doesn't require much more than a {@link DataSource}
 * to run.
 *
 * @author Guillaume Mary
 */
public class StalactitePlatformTransactionManager extends JdbcTransactionManager implements ConnectionConfiguration.TransactionalConnectionProvider {
	
	private final Set<CommitListener> commitListeners = new KeepOrderSet<>();
	private final Set<RollbackListener> rollbackListeners = new KeepOrderSet<>();
	
	public StalactitePlatformTransactionManager(DataSource dataSource) {
		super(dataSource);
	}
	
	@Override
	public void addCommitListener(CommitListener commitListener) {
		this.commitListeners.add(commitListener);
	}
	
	@Override
	public void addRollbackListener(RollbackListener rollbackListener) {
		this.rollbackListeners.add(rollbackListener);
	}
	
	/**
	 * Implemented to make it get the connection from current transaction given by Spring {@link TransactionSynchronizationManager}
	 *
	 * @return the current {@link Connection} available in Spring transaction context.
	 */
	@Override
	public Connection giveConnection() {
		if (!TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("No active transaction");
		} else {
			ConnectionHolder resource = (ConnectionHolder) TransactionSynchronizationManager.getResource(getDataSource());
			if (resource == null) {
				throw new IllegalStateException("No connection available");
			} else {
				return resource.getConnection();
			}
		}
	}
	
	@Override
	protected void prepareSynchronization(DefaultTransactionStatus status, TransactionDefinition definition) {
		super.prepareSynchronization(status, definition);
		if (status.isNewSynchronization()) {
			this.commitListeners.forEach(commitListener -> TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
				@Override
				public void beforeCommit(boolean readOnly) {
					commitListener.beforeCommit();
				}
				
				@Override
				public void afterCommit() {
					commitListener.afterCommit();
				}
			}));
			this.rollbackListeners.forEach(rollbackListener -> TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
				
				@Override
				public void afterCompletion(int status) {
					if (status == STATUS_ROLLED_BACK) {
						rollbackListener.afterRollback();
					}
				}
				
				// Note that we can't implement a call to beforeRollback() because TransactionSynchronization doesn't propose it
				// The closest method would be beforeCompletion() but it is also invoked on beforeCommit()
				
			}));
		}
	}
	
	/**
	 * Implemented to ask for a new transaction to Spring context and run given {@link org.codefilarete.stalactite.engine.SeparateTransactionExecutor.JdbcOperation}
	 * in it.
	 * @param jdbcOperation a sql operation that will call {@link #giveConnection()} to execute its statements.
	 */
	@Override
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(this);
		transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcOperation.execute(giveConnection());
			}
		});
	}
}
