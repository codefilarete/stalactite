package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.engine.PersistenceContextITTest;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.MariaDBDialect;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextMariaDBTest extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new MariaDBEmbeddableDataSource(3307);
	}
	
	@Override
	protected Dialect createDialect() {
		return new MariaDBDialect();
	}
}