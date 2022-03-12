package org.codefilarete.stalactite.persistence.engine.runtime;

import javax.sql.DataSource;

import org.codefilarete.stalactite.persistence.engine.PersistenceContextITTest;
import org.codefilarete.stalactite.persistence.sql.DerbyDialect;
import org.codefilarete.stalactite.persistence.sql.Dialect;
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