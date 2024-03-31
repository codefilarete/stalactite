package org.codefilarete.stalactite.sql.statement.binder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Set;

import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class SQLiteParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new SQLiteInMemoryDataSource();
	}
	
	@Override
	@BeforeEach
	void createParameterBinderRegistry() {
		parameterBinderRegistry = new SQLiteParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	void createJavaTypeToSqlTypeMapping() {
		javaTypeToSqlTypeMapping = new SQLiteTypeMapping();
	}
	
	/**
	 * Overridden to take into account rounding made by SQLite on stored nanos
	 */
	@Test
	void localDateTimeBinder() throws SQLException {
		LocalDateTime initialTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456789);
		// SQLite cuts the last figures (impossible to find an appropriate type), so it must be compared to 123457000, not 123456000
		LocalDateTime comparisonTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123_000_000);
		Set<LocalDateTime> databaseContent = insertAndSelect(LocalDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	/**
	 * Overridden to take into account rounding made by SQLite on stored nanos
	 */
	@Test
	void localTimeBinder() throws SQLException {
		LocalTime initialTime = LocalTime.of(4, 23, 35, 123456789);
		// SQLite cuts the last figures (impossible to find an appropriate type), so it must be compared to 123457000, not 123456000
		LocalTime comparisonTime = LocalTime.of(4, 23, 35, 123_000_000);
		Set<LocalTime> databaseContent = insertAndSelect(LocalTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
}