package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.MariaDBDialect;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;

/**
 * @author Guillaume Mary
 */
public class PersisterMariaDBTest extends PersisterITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
    @Override
	public DataSource giveDataSource() {
        return DATASOURCE;
    }
	
	@Override
	Dialect createDialect() {
		return new MariaDBDialect();
	}
}
