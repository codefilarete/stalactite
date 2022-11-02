package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

class HSQLDBDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource HSQLDataSource = new HSQLDBInMemoryDataSource();
		HSQLDBDialect HSQLDialect = new HSQLDBDialect();
		HSQLDBDDLTableGenerator testInstance = (HSQLDBDDLTableGenerator) HSQLDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, HSQLDataSource.getConnection());
	}
	
}