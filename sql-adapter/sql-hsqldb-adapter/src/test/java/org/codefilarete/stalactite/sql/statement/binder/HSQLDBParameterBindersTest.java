package org.codefilarete.stalactite.sql.statement.binder;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBDatabaseHelper;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class HSQLDBParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new HSQLDBDatabaseHelper();
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
	
	@Test
	void zonedDateTimeBinder() throws SQLException {
		ZonedDateTime initialTime = ZonedDateTime.of(2024, 6, 18, 11, 22, 33, 123456789, ZoneOffset.ofHours(5));
		// HSQLDB doesn't store last nanos figures, so it must be compared to 123456000, not 123456789
		ZonedDateTime comparisonTime = ZonedDateTime.of(2024, 6, 18, 11, 22, 33, 123456000, ZoneOffset.ofHours(5));
		Set<ZonedDateTime> databaseContent = insertAndSelect(ZonedDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	@Test
	void offsetDateTimeBinder() throws SQLException {
		OffsetDateTime initialTime = OffsetDateTime.of(2024, 6, 18, 11, 22, 33, 123456789, ZoneOffset.ofHours(5));
		// HSQLDB doesn't store last nanos figures, so it must be compared to 123456000, not 123456789
		OffsetDateTime comparisonTime = OffsetDateTime.of(2024, 6, 18, 11, 22, 33, 123456000, ZoneOffset.ofHours(5));
		Set<OffsetDateTime> databaseContent = insertAndSelect(OffsetDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
}