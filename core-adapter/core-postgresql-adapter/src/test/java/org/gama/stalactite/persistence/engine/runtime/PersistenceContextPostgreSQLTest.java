package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.engine.PersistenceContextITTest;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.PostgreSQLDialect;
import org.gama.stalactite.sql.test.PostgreSQLEmbeddedDataSource;

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