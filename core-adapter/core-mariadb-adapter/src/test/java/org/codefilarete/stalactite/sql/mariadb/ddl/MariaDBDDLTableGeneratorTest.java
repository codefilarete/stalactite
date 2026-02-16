package org.codefilarete.stalactite.sql.mariadb.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLTableGeneratorTest;
import org.codefilarete.stalactite.sql.mariadb.MariaDBDialectBuilder;
import org.codefilarete.stalactite.sql.mariadb.test.MariaDBTestDataSourceSelector;
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
