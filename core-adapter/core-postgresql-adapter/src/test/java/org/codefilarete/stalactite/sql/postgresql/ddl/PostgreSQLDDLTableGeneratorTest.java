package org.codefilarete.stalactite.sql.postgresql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLTableGeneratorTest;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDatabaseSettings.PostgreSQLDDLTableGenerator;
import org.codefilarete.stalactite.sql.postgresql.PostgreSQLDialectBuilder;
import org.codefilarete.stalactite.sql.postgresql.test.PostgreSQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

class PostgreSQLDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource postgreSQLDataSource = new PostgreSQLTestDataSourceSelector().giveDataSource();
		Dialect dialect = PostgreSQLDialectBuilder.defaultPostgreSQLDialect();
		PostgreSQLDDLTableGenerator testInstance = (PostgreSQLDDLTableGenerator) dialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, postgreSQLDataSource.getConnection());
	}
	
}
