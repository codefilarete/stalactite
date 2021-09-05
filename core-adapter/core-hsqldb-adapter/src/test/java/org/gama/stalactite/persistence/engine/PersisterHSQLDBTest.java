package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterHSQLDBTest extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new HSQLDBInMemoryDataSource();
    }
	
	@Override
	Dialect createDialect() {
		return new HSQLDBDialect();
	}
}
