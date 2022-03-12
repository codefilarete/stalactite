package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
class SelectExecutorMariaDBTest extends SelectExecutorITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
}
