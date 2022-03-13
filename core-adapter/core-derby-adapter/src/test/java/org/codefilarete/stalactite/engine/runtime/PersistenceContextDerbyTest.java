package org.codefilarete.stalactite.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.sql.DerbyDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextDerbyTest extends PersistenceContextITTest {

	@Override
	protected DataSource giveDataSource() {
		return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
	}
	
	@Override
	protected Dialect createDialect() {
		return new DerbyDialect();
	}
}