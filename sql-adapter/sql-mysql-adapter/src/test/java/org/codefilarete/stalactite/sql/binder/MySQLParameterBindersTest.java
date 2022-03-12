package org.codefilarete.stalactite.sql.binder;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public class MySQLParameterBindersTest extends AbstractParameterBindersITTest {
	
	private static final DataSource DATASOURCE = new MySQLTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new MySQLParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new MySQLTypeMapping();
	}
}