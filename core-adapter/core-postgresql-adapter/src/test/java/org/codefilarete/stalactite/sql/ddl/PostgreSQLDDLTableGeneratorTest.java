package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.PostgreSQLDialect;
import org.codefilarete.stalactite.sql.PostgreSQLDialect.PostgreSQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.test.PostgreSQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

class PostgreSQLDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource postgreSQLDataSource = new PostgreSQLTestDataSourceSelector().giveDataSource();
		PostgreSQLDialect PostgreSQLDialect = new PostgreSQLDialect();
		PostgreSQLDDLTableGenerator testInstance = (PostgreSQLDDLTableGenerator) PostgreSQLDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, postgreSQLDataSource.getConnection());
	}
	
}