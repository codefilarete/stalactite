package org.codefilarete.stalactite.sql.statement.binder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleTestDataSourceSelector;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class OracleParameterBindersTest extends AbstractParameterBindersITTest {
	
	private static final DataSource DATASOURCE = new OracleTestDataSourceSelector().giveDataSource();
	
	@Override
	public DataSource giveDataSource() {
		return DATASOURCE;
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new OracleDatabaseHelper();
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new OracleParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new OracleTypeMapping();
	}
	
	/**
	 * Overridden to take into account rounding made by Oracle on stored nanos
	 */
	@Test
	void localDateTimeBinder() throws SQLException {
		LocalDateTime initialTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456789);
		// Oracle rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalDateTime comparisonTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123457000);
		Set<LocalDateTime> databaseContent = insertAndSelect(LocalDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	/**
	 * Overridden to take into account rounding made by Oracle on stored nanos
	 */
	@Test
	void localTimeBinder() throws SQLException {
		LocalTime initialTime = LocalTime.of(4, 23, 35, 123456789);
		// Oracle rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalTime comparisonTime = LocalTime.of(4, 23, 35, 123457000);
		Set<LocalTime> databaseContent = insertAndSelect(LocalTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	@Test
	void zonedDateTimeBinder() throws SQLException {
		ZonedDateTime initialTime = ZonedDateTime.of(2024, 6, 18, 11, 22, 33, 123456789, ZoneOffset.ofHours(5));
		// Oracle rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		ZonedDateTime comparisonTime = ZonedDateTime.of(2024, 6, 18, 11, 22, 33, 123457000, ZoneOffset.ofHours(5));
		Set<ZonedDateTime> databaseContent = insertAndSelect(ZonedDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	@Test
	void offsetDateTimeBinder() throws SQLException {
		OffsetDateTime initialTime = OffsetDateTime.of(2024, 6, 18, 11, 22, 33, 123456789, ZoneOffset.ofHours(5));
		// Oracle rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		OffsetDateTime comparisonTime = OffsetDateTime.of(2024, 6, 18, 11, 22, 33, 123457000, ZoneOffset.ofHours(5));
		Set<OffsetDateTime> databaseContent = insertAndSelect(OffsetDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
}