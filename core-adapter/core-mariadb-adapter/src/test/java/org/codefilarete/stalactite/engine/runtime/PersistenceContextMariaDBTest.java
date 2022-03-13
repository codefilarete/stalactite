package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.MariaDBDialect;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextMariaDBTest extends PersistenceContextITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	protected DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected Dialect createDialect() {
		return new MariaDBDialect();
	}
}