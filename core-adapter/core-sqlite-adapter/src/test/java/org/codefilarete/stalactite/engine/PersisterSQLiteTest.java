package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SQLiteDialectBuilder;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterSQLiteTest extends PersisterITTest {
	
	@Override
	public DataSource giveDataSource() {
        return new SQLiteInMemoryDataSource();
    }
	
	@Override
	Dialect createDialect() {
		return SQLiteDialectBuilder.defaultSQLiteDialect();
	}
}
