package org.gama.stalactite.sql.binder;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class PostgreSQLParameterBindersTest extends AbstractParameterBindersITTest {

	@Override
	@BeforeEach
	public void createDataSource() {
		dataSource = new PostgreSQLEmbeddedDataSource(5431);
	}

	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		super.parameterBinderRegistry = new PostgreSQLParameterBinderRegistry();
	}

	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		super.javaTypeToSqlTypeMapping = new PostgreSQLTypeMapping();
	}

	@Test
	void blobBinder() throws SQLException {
		Blob blob = new InMemoryBlobSupport("Hello world !".getBytes());
		Set<Blob> valuesToInsert = Arrays.asSet(blob, null);
		Set<Blob> databaseContent = insertAndSelect(Blob.class, valuesToInsert);
		assertThat(convertBlobToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
	}

	protected <T> Set<T> insertAndSelect(Class<T> typeToTest, Set<T> valuesToInsert) throws SQLException {
		ParameterBinder<T> testInstance = parameterBinderRegistry.getBinder(typeToTest);
		String sqlColumnType = javaTypeToSqlTypeMapping.getTypeName(typeToTest);
		Connection connection = dataSource.getConnection();
		// short hack because Large Object should not be used in auto-commit mode (PostgreSQL limitation)
		if (typeToTest == Blob.class) {
			connection.setAutoCommit(false);
		}
		return insertAndSelect(testInstance, sqlColumnType, valuesToInsert, connection);
	}
}