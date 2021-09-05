package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.engine.PersistenceContextITTest;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.MySQLDialect;
import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;

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