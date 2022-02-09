package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MySQLDialect;
import org.codefilarete.stalactite.sql.test.MySQLEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterMySQLTest extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new MySQLEmbeddableDataSource(3307);
    }
	
	@Override
	Dialect createDialect() {
		return new MySQLDialect();
	}
}
