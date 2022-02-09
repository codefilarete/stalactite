package org.codefilarete.stalactite.sql.binder;

import org.codefilarete.stalactite.sql.test.MariaDBEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;

/**
 * @author Guillaume Mary
 */
class MariaDBParameterBindersTest extends AbstractParameterBindersITTest {

	@Override
	@BeforeEach
	void createDataSource() {
		dataSource = new MariaDBEmbeddableDataSource(3406);
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

//	@Test
//	@Override
//	void localDateTimeBinder() throws SQLException {
//		LocalDateTime now = LocalDateTime.now();
//		LocalDateTime comparisonInstant = now.minusNanos(now.getNano());
//		testParameterBinder(LocalDateTime.class, Arrays.asSet(null, comparisonInstant));
//	}

//	@Test
//	@Override
//	void timestampBinder() throws SQLException {
//		String sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
//		testParameterBinder(parameterBinderRegistry.getBinder(Timestamp.class), sqlColumnType, Arrays.asSet(null, new Timestamp(System.currentTimeMillis())));
////		testParameterBinder(parameterBinderRegistry.getBinder(LocalDateTime.class), sqlColumnType, Arrays.asSet(null, LocalDateTime.now()));
//		
////		LocalDateTime now = LocalDateTime.now();
////		Timestamp comparisonInstant = java.sql.Timestamp.valueOf(now.minusNanos(now.getNano()));
////		testParameterBinder(Timestamp.class, Arrays.asSet(null, comparisonInstant));
//	}
}