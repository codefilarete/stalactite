package org.stalactite.benchmark;

import java.sql.Connection;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.stalactite.persistence.engine.TransactionManager;

/**
 * @author Guillaume Mary
 */
public class SpringTransactionManager implements TransactionManager {
	
	private final DataSourceTransactionManager transactionManager;
	
	public SpringTransactionManager(DataSourceTransactionManager dataSourceTransactionManager) {
		this.transactionManager = dataSourceTransactionManager;
	}
	
	
	@Override
	public Connection getCurrentConnection() {
		return DataSourceUtils.getConnection(transactionManager.getDataSource());
	}
	
	@Override
	public void executeInNewTransaction(final JdbcOperation jdbcOperation) {
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
