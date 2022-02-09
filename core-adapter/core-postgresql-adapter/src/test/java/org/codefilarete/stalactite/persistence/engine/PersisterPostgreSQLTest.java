package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.PostgreSQLDialect;
import org.codefilarete.stalactite.sql.test.PostgreSQLEmbeddedDataSource;

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
