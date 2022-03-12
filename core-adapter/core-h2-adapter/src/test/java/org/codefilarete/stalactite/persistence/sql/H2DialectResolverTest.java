package org.codefilarete.stalactite.persistence.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class H2DialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle H2 1.4
	 */
	@Test
	void resolve_1_4() throws SQLException {
		DataSource h2DataSource = new H2InMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(h2DataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(H2Dialect.class);
	}
	
}