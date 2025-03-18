package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.DerbyDialectBuilder;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.Test;

class DerbyDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource HSQLDataSource = new DerbyInMemoryDataSource();
		Dialect derbyDialect = DerbyDialectBuilder.defaultDerbyDialect();
		DerbyDDLTableGenerator testInstance = (DerbyDDLTableGenerator) derbyDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, HSQLDataSource.getConnection());
	}
	
}