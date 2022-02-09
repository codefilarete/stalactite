package org.codefilarete.stalactite.sql.binder;

import org.codefilarete.stalactite.sql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
public class MySQLParameterBindersTest extends AbstractParameterBindersITTest {

	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new MySQLEmbeddableDataSource(3406);
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