package org.codefilarete.stalactite.sql.postgresql.statement.binder;

import javax.sql.DataSource;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Set;

import org.codefilarete.stalactite.sql.statement.binder.AbstractParameterBindersITTest;
import org.codefilarete.stalactite.sql.statement.binder.InMemoryBlobSupport;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLDatabaseHelper;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class PostgreSQLParameterBindersTest extends AbstractParameterBindersITTest {
	
	private static final DataSource DATASOURCE = new PostgreSQLTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new PostgreSQLDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	protected void createParameterBinderRegistry() {
		super.parameterBinderRegistry = new PostgreSQLParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	protected void createJavaTypeToSqlTypeMapping() {
		super.javaTypeToSqlTypeMapping = new PostgreSQLTypeMapping();
	}
	
	@Test
	@Override
	protected void blobBinder() throws SQLException {
		Blob blob = new InMemoryBlobSupport("Hello world !".getBytes());
		Set<Blob> valuesToInsert = Arrays.asSet(blob, null);
		Connection connection = connectionProvider.giveConnection();
		// short hack because Large Object should not be used in auto-commit mode (PostgreSQL limitation)
		connection.setAutoCommit(false);
		Set<Blob> databaseContent = insertAndSelect(Blob.class, valuesToInsert);
		assertThat(convertBlobToString(databaseContent)).isEqualTo(Arrays.asSet(null, "Hello world !"));
		connection.rollback();	// release Transaction to avoid blocking next tests
	}
	
	/**
	 * Overridden to take into account rounding made by PostgreSQL on stored nanos
	 */
	@Test
	@Override
	protected void localDateTimeBinder() throws SQLException {
		LocalDateTime initialTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456789);
		// PostgreSQL rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalDateTime comparisonTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123457000);
		Set<LocalDateTime> databaseContent = insertAndSelect(LocalDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	/**
	 * Overridden to take into account rounding made by PostgreSQL on stored nanos
	 */
	@Test
	@Override
	protected void localTimeBinder() throws SQLException {
		LocalTime initialTime = LocalTime.of(4, 23, 35, 123456789);
		// PostgreSQL rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalTime comparisonTime = LocalTime.of(4, 23, 35, 123457000);
		Set<LocalTime> databaseContent = insertAndSelect(LocalTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
}
