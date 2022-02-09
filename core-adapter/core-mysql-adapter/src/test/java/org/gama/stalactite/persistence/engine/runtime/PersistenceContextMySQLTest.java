package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MySQLDialect;
import org.codefilarete.stalactite.sql.test.MySQLEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextMySQLTest extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new MySQLEmbeddableDataSource(3307);
	}
	
	@Override
	protected Dialect createDialect() {
		return new MySQLDialect();
	}
}