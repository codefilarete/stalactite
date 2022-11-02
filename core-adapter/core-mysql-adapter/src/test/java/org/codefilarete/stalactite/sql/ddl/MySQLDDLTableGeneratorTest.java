package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.MySQLDialect;
import org.codefilarete.stalactite.sql.test.MySQLTestDataSourceSelector;
import org.junit.jupiter.api.Test;

class MySQLDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource mariaDBDataSource = new MySQLTestDataSourceSelector().giveDataSource();
		MySQLDialect mariaDBDialect = new MySQLDialect();
		MySQLDDLTableGenerator testInstance = (MySQLDDLTableGenerator) mariaDBDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, mariaDBDataSource.getConnection());
	}
	
}