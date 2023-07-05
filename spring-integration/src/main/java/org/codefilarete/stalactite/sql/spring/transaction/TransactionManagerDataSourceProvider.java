package org.codefilarete.stalactite.sql.spring.transaction;

import javax.sql.DataSource;

/**
 * Marking interface saying that we need a {@link DataSource} coming from a {@link org.springframework.transaction.PlatformTransactionManager}
 * 
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface TransactionManagerDataSourceProvider {
	
	DataSource getDataSource();
}
