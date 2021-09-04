package org.gama.stalactite.sql.binder;

import binder.H2ParameterBinderRegistry;
import binder.H2TypeMapping;
import org.gama.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class H2ParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new H2InMemoryDataSource();
	}

	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		super.parameterBinderRegistry = new H2ParameterBinderRegistry();
	}

	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		super.javaTypeToSqlTypeMapping = new H2TypeMapping();
	}
}