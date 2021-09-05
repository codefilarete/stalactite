package org.gama.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.engine.PersistenceContextITTest;
import org.gama.stalactite.persistence.sql.DerbyDialect;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextDerbyTest extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected Dialect createDialect() {
		return new DerbyDialect();
	}
}