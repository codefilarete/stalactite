package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SQLiteDialect;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextSQLiteTest extends PersistenceContextITTest {

	@Override
	protected DataSource giveDataSource() {
		return new SQLiteInMemoryDataSource();
	}
	
	@Override
	protected Dialect createDialect() {
		return new SQLiteDialect();
	}
}