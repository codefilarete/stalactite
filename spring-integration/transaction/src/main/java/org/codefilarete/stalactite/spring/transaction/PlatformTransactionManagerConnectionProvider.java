package org.codefilarete.stalactite.spring.transaction;

import java.sql.Connection;

import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link ConnectionProvider} acting as a bridge for Spring's {@link PlatformTransactionManager}.
 * One subclass for each known {@link PlatformTransactionManager} exists, prefer them to avoid {@link NoClassDefFoundError} :
 * {@link PlatformTransactionManagerConnectionProvider} could have handled those cases through some constructor or factory method, but it rises an
 * error if the project doesn't have the dependencies due to JPA / Hibernate presence in imports.
 * This class is left public to let one handle an unexpected case.
 * 
 * @author Guillaume Mary
 * @see DataSourceConnectionProvider
 * @see Hibernate5ConnectionProvider
 * @see JpaConnectionProvider
 */
public class PlatformTransactionManagerConnectionProvider implements SeparateTransactionExecutor {
	
	/** {@link PlatformTransactionManager} given at construction time. Used for new transaction building. */
	private final PlatformTransactionManager transactionManager;
	
	/** {@link javax.sql.DataSource} extractor from the transaction manager */
	private final TransactionManagerDataSourceProvider dataSourceProvider;
	
	public PlatformTransactionManagerConnectionProvider(PlatformTransactionManager transactionManager, TransactionManagerDataSourceProvider dataSourceProvider) {
		this.transactionManager = transactionManager;
		this.dataSourceProvider = dataSourceProvider;
	}
	
	@Override
	public Connection giveConnection() {
		// DataSourceUtils.getConnection(..) gets a connection even if no surrounding transaction exists so we have to check it before
		if (!TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("No active transaction");
		} else {
			return DataSourceUtils.getConnection(dataSourceProvider.getDataSource());
		}
	}
	
	@Override
	public void executeInNewTransaction(JdbcOperation jdbcOperation) {
		TransactionTemplate transactionTemplate = new TransactionTemplate(this.transactionManager);
		transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		transactionTemplate.execute(new TransactionCallbackWithoutResult() {
			@Override
			protected void doInTransactionWithoutResult(TransactionStatus status) {
				jdbcOperation.execute();
			}
		});
	}
}
