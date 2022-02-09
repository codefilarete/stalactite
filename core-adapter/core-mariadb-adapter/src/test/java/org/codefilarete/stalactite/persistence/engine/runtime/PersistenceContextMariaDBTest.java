package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MariaDBDialect;
import org.codefilarete.stalactite.sql.test.MariaDBEmbeddableDataSource;

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