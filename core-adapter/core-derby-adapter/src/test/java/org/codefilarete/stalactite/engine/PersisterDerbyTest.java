package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.DerbyDialectBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterDerbyTest extends PersisterITTest {
	
	@Override
	public DataSource giveDataSource() {
        return new DerbyInMemoryDataSource();
    }
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
	}
	
	@Override
	Dialect createDialect() {
		return DerbyDialectBuilder.defaultDerbyDialect();
	}
}
