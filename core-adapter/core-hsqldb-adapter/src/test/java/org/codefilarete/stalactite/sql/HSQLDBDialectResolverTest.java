package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class HSQLDBDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle HSQLDB 2.0
	 */
	@Test
	void resolve_2_0() throws SQLException {
		DataSource hsqldbDataSource = new HSQLDBInMemoryDataSource();

		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(hsqldbDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(HSQLDBDialect.class);
	}
}