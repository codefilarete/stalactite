package org.codefilarete.stalactite.sql.mariadb.statement.binder;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.statement.binder.AbstractParameterBindersITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.mariadb.test.MariaDBDatabaseHelper;
import org.codefilarete.stalactite.sql.mariadb.test.MariaDBTestDataSourceSelector;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class MariaDBParameterBindersTest extends AbstractParameterBindersITTest {
	
	private static final DataSource DATASOURCE = new MariaDBTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	protected DatabaseHelper giveDatabaseHelper() {
		return new MariaDBDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	protected void createParameterBinderRegistry() {
		parameterBinderRegistry = new MariaDBParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	protected void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new MariaDBTypeMapping();
	}
}
