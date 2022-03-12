package org.codefilarete.stalactite.persistence.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class DerbyDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle Apache Derby 10.14
	 */
	@Test
	void resolve_10_14() throws SQLException {
		DataSource derbyDataSource = new DerbyInMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(derbyDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(DerbyDialect.class);
	}
}