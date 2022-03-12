package org.codefilarete.stalactite.sql.binder;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
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
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new MariaDBParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new MariaDBTypeMapping();
	}
}