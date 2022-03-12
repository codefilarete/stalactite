package org.codefilarete.stalactite.sql.spring;

import org.springframework.orm.hibernate4.HibernateTransactionManager;

/**
 * @author Guillaume Mary
 */
public class Hibernate4ConnectionProvider extends PlatformTransactionManagerConnectionProvider {
	
	public Hibernate4ConnectionProvider(HibernateTransactionManager transactionManager) {
		super(transactionManager, transactionManager::getDataSource);
	}
}
