package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;

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
