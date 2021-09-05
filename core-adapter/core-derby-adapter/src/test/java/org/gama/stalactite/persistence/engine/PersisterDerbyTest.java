package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.sql.DerbyDialect;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;

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
