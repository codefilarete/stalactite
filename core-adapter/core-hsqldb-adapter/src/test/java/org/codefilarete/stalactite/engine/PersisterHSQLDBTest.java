package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterHSQLDBTest extends PersisterITTest {
	
    @Override
	public DataSource giveDataSource() {
        return new HSQLDBInMemoryDataSource();
    }
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
	
	@Override
	Dialect createDialect() {
		return HSQLDBDialectBuilder.defaultHSQLDBDialect();
	}
}
