package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.MariaDBDialectBuilder;
import org.codefilarete.stalactite.sql.test.MariaDBTestDataSourceSelector;
import org.junit.jupiter.api.Test;

class MariaDBDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource mariaDBDataSource = new MariaDBTestDataSourceSelector().giveDataSource();
		Dialect mariaDBDialect = MariaDBDialectBuilder.defaultMariaDBDialect();
		MariaDBDDLTableGenerator testInstance = (MariaDBDDLTableGenerator) mariaDBDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, mariaDBDataSource.getConnection());
	}
}