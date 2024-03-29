package org.codefilarete.stalactite.sql.spring.transaction;

import org.springframework.orm.jpa.JpaTransactionManager;

/**
 * @author Guillaume Mary
 */
public class JpaConnectionProvider extends PlatformTransactionManagerConnectionProvider {
	
	public JpaConnectionProvider(JpaTransactionManager transactionManager) {
		super(transactionManager, transactionManager::getDataSource);
	}
}
