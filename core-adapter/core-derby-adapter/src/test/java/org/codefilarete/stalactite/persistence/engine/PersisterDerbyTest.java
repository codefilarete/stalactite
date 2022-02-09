package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.DerbyDialect;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterDerbyTest extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new DerbyInMemoryDataSource();
    }
	
	@Override
	Dialect createDialect() {
		return new DerbyDialect();
	}
}
