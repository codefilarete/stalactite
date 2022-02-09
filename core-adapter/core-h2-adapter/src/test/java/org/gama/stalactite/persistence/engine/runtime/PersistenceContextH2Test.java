package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.H2Dialect;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextH2Test extends PersistenceContextITTest {

	@Override
	protected DataSource createDataSource() {
		return new H2InMemoryDataSource();
	}
	
	@Override
	protected Dialect createDialect() {
		return new H2Dialect();
	}
}