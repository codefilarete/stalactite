package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class MariaDBDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle MariaDB 10.0
	 */
	@Test
	void resolve_10_0() throws SQLException {
		DataSource mariaDBDataSource = new MariaDBTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(mariaDBDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(MariaDBDialect.class);
	}
	
}