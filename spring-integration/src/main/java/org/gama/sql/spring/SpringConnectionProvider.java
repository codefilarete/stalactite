package org.gama.sql.spring;

import javax.annotation.Nonnull;
import java.sql.Connection;

import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * A {@link org.gama.sql.ConnectionProvider} for Spring framework
 * 
 * @author Guillaume Mary
 */
public class SpringConnectionProvider implements SeparateTransactionExecutor {
	
	private final DataSourceTransactionManager transactionManager;
	
	public SpringConnectionProvider(DataSourceTransactionManager dataSourceTransactionManager) {
		this.transactionManager = dataSourceTransactionManager;
	}
	
	@Nonnull
	@Override
	public Connection getCurrentConnection() {
		// DataSourceUtils.getConnection(..) gets a connection even if no surrounding transaction exists so we have to check it before
		if (!TransactionSynchronizationManager.isActualTransactionActive()) {
			throw new IllegalStateException("No active transaction");
		} else {
			return DataSourceUtils.getConnection(transactionManager.getDataSource());
		}
	}
	
	@Override
	public void executeInNewTransaction(SeparateTransactionExecutor.JdbcOperation jdbcOperation) {
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