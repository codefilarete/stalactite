package org.gama.sql.spring;

import javax.sql.DataSource;

/**
 * @author Guillaume Mary
 */
@FunctionalInterface
public interface TransactionManagerDataSourceProvider {
	
	DataSource getDataSource();
}
