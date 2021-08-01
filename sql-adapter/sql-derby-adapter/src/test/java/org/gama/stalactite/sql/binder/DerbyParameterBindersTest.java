package org.gama.stalactite.sql.binder;

import org.gama.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public class DerbyParameterBindersTest extends AbstractParameterBindersITTest {

	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new DerbyInMemoryDataSource();
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