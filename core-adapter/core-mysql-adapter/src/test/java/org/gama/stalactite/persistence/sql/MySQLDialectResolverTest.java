package org.gama.stalactite.persistence.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.gama.stalactite.sql.test.MySQLEmbeddableDataSource;
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
		DataSource mySQLDataSource = new MySQLEmbeddableDataSource(3307);
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(mySQLDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(MySQLDialect.class);
	}
	
}