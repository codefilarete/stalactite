package org.codefilarete.stalactite.persistence.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.PostgreSQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class PostgreSQLDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle PostgreSQL 9.6
	 */
	@Test
	void resolve_9_6() throws SQLException {
		DataSource postgresqlDataSource = new PostgreSQLTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(postgresqlDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(PostgreSQLDialect.class);
	}
}