package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class SQLiteDialectResolverTest {
	
	/**
	 * Integration test to ensure that an entry is registered to handle SQLite 3.45
	 */
	@Test
	void resolve_3_45() throws SQLException {
		DataSource SQLiteDataSource = new SQLiteInMemoryDataSource();
		
		ServiceLoaderDialectResolver dialectResolver = new ServiceLoaderDialectResolver();
		Dialect dialect = dialectResolver.determineDialect(SQLiteDataSource.getConnection());
		Assertions.assertThat(dialect).isExactlyInstanceOf(SQLiteDialect.class);
	}
}