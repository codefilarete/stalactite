package org.codefilarete.stalactite.sql.h2.statement.binder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Set;

import org.codefilarete.stalactite.sql.statement.binder.AbstractParameterBindersITTest;
import org.codefilarete.stalactite.sql.test.DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2DatabaseHelper;
import org.codefilarete.stalactite.sql.h2.test.H2InMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class H2ParameterBindersTest extends AbstractParameterBindersITTest {
	
	@Override
	public DataSource giveDataSource() {
		return new H2InMemoryDataSource();
	}
	
	@Override
	protected DatabaseHelper giveDatabaseHelper() {
		return new H2DatabaseHelper();
	}
	
	@Override
	@BeforeEach
	protected void createParameterBinderRegistry() {
		super.parameterBinderRegistry = new H2ParameterBinderRegistry();
	}
	
	@Override
	@BeforeEach
	protected void createJavaTypeToSqlTypeMapping() {
		super.javaTypeToSqlTypeMapping = new H2TypeMapping();
	}
	
	/**
	 * Overridden to take into account rounding made by H2 on stored nanos
	 */
	@Test
	@Override
	protected void localDateTimeBinder() throws SQLException {
		LocalDateTime initialTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123456789);
		// H2 rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalDateTime comparisonTime = LocalDateTime.of(2021, Month.JULY, 12, 4, 23, 35, 123457000);
		Set<LocalDateTime> databaseContent = insertAndSelect(LocalDateTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
	
	/**
	 * Overridden to take into account rounding made by H2 on stored nanos
	 */
	@Test
	@Override
	protected void localTimeBinder() throws SQLException {
		LocalTime initialTime = LocalTime.of(4, 23, 35, 123456789);
		// H2 rounds nanos to upper one when necessary, so it must be compared to 123457000, not 123456000
		LocalTime comparisonTime = LocalTime.of(4, 23, 35, 123457000);
		Set<LocalTime> databaseContent = insertAndSelect(LocalTime.class, Arrays.asSet(null, initialTime));
		assertThat(databaseContent).isEqualTo(Arrays.asSet(null, comparisonTime));
	}
}
