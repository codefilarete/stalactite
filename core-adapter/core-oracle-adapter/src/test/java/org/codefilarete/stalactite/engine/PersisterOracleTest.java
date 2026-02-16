package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.oracle.OracleDialectBuilder;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.oracle.test.OracleEmbeddableDataSource;

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
		return OracleDialectBuilder.defaultOracleDialect();
	}
}
