package org.codefilarete.stalactite.sql.hsqldb.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLTableGeneratorTest;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

class HSQLDBDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource HSQLDataSource = new HSQLDBInMemoryDataSource();
		Dialect HSQLDialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		HSQLDBDDLTableGenerator testInstance = (HSQLDBDDLTableGenerator) HSQLDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, HSQLDataSource.getConnection());
	}
	
}
