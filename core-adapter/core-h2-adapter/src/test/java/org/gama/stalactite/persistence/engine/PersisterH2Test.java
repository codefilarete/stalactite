package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.H2Dialect;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterH2Test extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new H2InMemoryDataSource();
    }
	
	@Override
	Dialect createDialect() {
		return new H2Dialect();
	}
}
