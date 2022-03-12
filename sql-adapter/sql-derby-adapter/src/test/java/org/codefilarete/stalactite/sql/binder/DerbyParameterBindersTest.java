package org.codefilarete.stalactite.sql.binder;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyDatabaseHelper;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public class DerbyParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new DerbyInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new DerbyDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new DerbyParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new DerbyTypeMapping();
	}
}