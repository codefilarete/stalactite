package org.codefilarete.stalactite.persistence.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class MySQLDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle MySQL 5.6
	 */
	@Test
	void resolve_5_6() throws SQLException {
		DataSource mySQLDataSource = new MySQLTestDataSourceSelector().giveDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(mySQLDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(MySQLDialect.class);
	}
	
}