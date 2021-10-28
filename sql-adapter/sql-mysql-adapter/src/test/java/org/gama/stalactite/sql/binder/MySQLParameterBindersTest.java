package org.gama.stalactite.sql.binder;

import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;
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

//	@Test
//	@Override
//	void localDateTimeBinder() throws SQLException {
//		LocalDateTime now = LocalDateTime.now();
//		LocalDateTime comparisonInstant = now.minusNanos(now.getNano());
//		testParameterBinder(LocalDateTime.class, Arrays.asSet(null, comparisonInstant));
//	}
}