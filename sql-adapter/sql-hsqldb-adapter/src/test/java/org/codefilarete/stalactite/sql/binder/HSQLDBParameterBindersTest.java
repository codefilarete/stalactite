package org.codefilarete.stalactite.sql.binder;

import javax.sql.DataSource;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class HSQLDBParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		super.parameterBinderRegistry = new HSQLDBParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		super.javaTypeToSqlTypeMapping = new HSQLDBTypeMapping();
	}
}