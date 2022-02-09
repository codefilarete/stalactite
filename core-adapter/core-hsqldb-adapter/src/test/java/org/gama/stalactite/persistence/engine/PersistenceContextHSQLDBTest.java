package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextHSQLDBTest extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected Dialect createDialect() {
		return new HSQLDBDialect();
	}
}