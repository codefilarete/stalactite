package org.gama.stalactite.sql.binder;

import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class HSQLDBParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new HSQLDBInMemoryDataSource();
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