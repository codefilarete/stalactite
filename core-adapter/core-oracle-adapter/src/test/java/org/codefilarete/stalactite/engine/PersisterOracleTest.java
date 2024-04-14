package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.OracleDialect;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleEmbeddableDataSource;

/**
 * @author Guillaume Mary
 */
public class PersisterOracleTest extends PersisterITTest {
	
    @Override
	public DataSource giveDataSource() {
        return new OracleEmbeddableDataSource();
    }
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new OracleDatabaseHelper();
	}
	
	@Override
	Dialect createDialect() {
		return new OracleDialect();
	}
}