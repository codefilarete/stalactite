package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.MariaDBDialect;
import org.gama.stalactite.sql.test.MariaDBEmbeddableDataSource;

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
