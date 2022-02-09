package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MariaDBDialect;
import org.codefilarete.stalactite.sql.test.MariaDBEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterMariaDBTest extends PersisterITTest {
	
    @Override
	DataSource createDataSource() {
        return new MariaDBEmbeddableDataSource(3307);
    }
	
	@Override
	Dialect createDialect() {
		return new MariaDBDialect();
	}
}
