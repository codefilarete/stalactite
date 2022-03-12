package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MariaDBDialect;
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