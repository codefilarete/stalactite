package org.codefilarete.stalactite.sql.spring.transaction;

import org.springframework.orm.hibernate5.HibernateTransactionManager;

/**
 * @author Guillaume Mary
 */
public class Hibernate5ConnectionProvider extends PlatformTransactionManagerConnectionProvider {
	
	public Hibernate5ConnectionProvider(HibernateTransactionManager transactionManager) {
		super(transactionManager, transactionManager::getDataSource);
	}
}
