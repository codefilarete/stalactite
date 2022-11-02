package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.H2Dialect;
import org.codefilarete.stalactite.sql.test.H2InMemoryDataSource;
import org.junit.jupiter.api.Test;

class H2DDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource HSQLDataSource = new H2InMemoryDataSource();
		H2Dialect HSQLDialect = new H2Dialect();
		H2DDLTableGenerator testInstance = (H2DDLTableGenerator) HSQLDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, HSQLDataSource.getConnection());
	}
	
}