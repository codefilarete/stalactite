package org.codefilarete.stalactite.sql.h2.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGeneratorTest;
import org.codefilarete.stalactite.sql.h2.H2DialectBuilder;
import org.codefilarete.stalactite.sql.h2.test.H2InMemoryDataSource;
import org.junit.jupiter.api.Test;

class H2DDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource HSQLDataSource = new H2InMemoryDataSource();
		Dialect dialect = H2DialectBuilder.defaultH2Dialect();
		DDLTableGenerator testInstance = dialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, HSQLDataSource.getConnection());
	}
}
