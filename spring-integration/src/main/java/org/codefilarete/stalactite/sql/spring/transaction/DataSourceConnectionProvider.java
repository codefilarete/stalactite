package org.codefilarete.stalactite.sql.spring.transaction;

import org.springframework.jdbc.datasource.DataSourceTransactionManager;

/**
 * @author Guillaume Mary
 */
public class DataSourceConnectionProvider extends PlatformTransactionManagerConnectionProvider {
	
	public DataSourceConnectionProvider(DataSourceTransactionManager transactionManager) {
		super(transactionManager, transactionManager::getDataSource);
	}
}
