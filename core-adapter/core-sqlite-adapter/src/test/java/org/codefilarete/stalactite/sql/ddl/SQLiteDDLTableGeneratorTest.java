package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.SQLiteDialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class SQLiteDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_throwsExceptionDueToForeignKeyCreation() throws SQLException {
		DataSource sqliteDataSource = new SQLiteInMemoryDataSource();
		SQLiteDialect sqliteDialect = new SQLiteDialect();
		SQLiteDDLTableGenerator testInstance = (SQLiteDDLTableGenerator) sqliteDialect.getDdlTableGenerator();
		
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance, new SimpleConnectionProvider(sqliteDataSource.getConnection()));
		ddlDeployer.getDdlGenerator().addTables(table1, table2);
		assertThatCode(ddlDeployer::deployDDL)
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("SQLite doesn't support foreign key creation out of create table");
	}
	
}