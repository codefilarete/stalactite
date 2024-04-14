package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.OracleEmbeddableDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class OracleDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle Oracle 23
	 */
	@Test
	void resolve_23() throws SQLException {
		DataSource oracleDataSource = new OracleEmbeddableDataSource();

		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(oracleDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(OracleDialect.class);
	}
}