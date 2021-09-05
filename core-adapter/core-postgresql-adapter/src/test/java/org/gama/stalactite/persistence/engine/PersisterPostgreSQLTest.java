package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.PostgreSQLDialect;
import org.gama.stalactite.sql.test.PostgreSQLEmbeddedDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterPostgreSQLTest extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new PostgreSQLEmbeddedDataSource();
    }
	
	@Override
	Dialect createDialect() {
		return new PostgreSQLDialect();
	}
}
