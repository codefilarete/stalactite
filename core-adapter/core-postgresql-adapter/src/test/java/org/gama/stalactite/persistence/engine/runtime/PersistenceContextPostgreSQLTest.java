package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.PostgreSQLDialect;
import org.codefilarete.stalactite.sql.test.PostgreSQLEmbeddedDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextPostgreSQLTest extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new PostgreSQLEmbeddedDataSource();
	}
	
	@Override
	protected Dialect createDialect() {
		return new PostgreSQLDialect();
	}
}