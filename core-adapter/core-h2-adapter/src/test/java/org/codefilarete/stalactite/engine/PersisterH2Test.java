package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.h2.H2DialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2InMemoryDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterH2Test extends PersisterITTest {
	
    @Override
	public DataSource giveDataSource() {
        return new H2InMemoryDataSource();
    }
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new H2DatabaseHelper();
	}
	
	@Override
	Dialect createDialect() {
		return H2DialectBuilder.defaultH2Dialect();
	}
}
