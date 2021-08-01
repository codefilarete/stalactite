package org.gama.stalactite.sql.binder;

import java.sql.SQLException;
import java.time.LocalDateTime;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

	@Test
	@Override
	void localDateTimeBinder() throws SQLException {
		String sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
		testParameterBinder(parameterBinderRegistry.getBinder(LocalDateTime.class), sqlColumnType, Arrays.asSet(null, LocalDateTime.now()));
	}

	@Test
	@Override
	void timestampBinder() throws SQLException {
		String sqlColumnType = "timestamp(3) null"; // 3 for nano precision, else nanos are lost during storage, hence comparison fails
		testParameterBinder(parameterBinderRegistry.getBinder(LocalDateTime.class), sqlColumnType, Arrays.asSet(null, LocalDateTime.now()));
	}
}