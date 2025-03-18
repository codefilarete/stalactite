package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SQLiteDialectBuilder;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

class SQLiteDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	@Test
	void generatedSQL_runOnAliveDatabase_throwsExceptionDueToForeignKeyCreation() throws SQLException {
		DataSource sqliteDataSource = new SQLiteInMemoryDataSource();
		Dialect sqliteDialect = SQLiteDialectBuilder.defaultSQLiteDialect();
		SQLiteDDLTableGenerator testInstance = (SQLiteDDLTableGenerator) sqliteDialect.getDdlTableGenerator();
		
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance, new DDLSequenceGenerator(new DMLNameProvider(Collections.emptyMap())), new SimpleConnectionProvider(sqliteDataSource.getConnection()));
		ddlDeployer.getDdlGenerator().addTables(table1, table2);
		assertThatCode(ddlDeployer::deployDDL)
				.isInstanceOf(UnsupportedOperationException.class)
				.hasMessage("SQLite doesn't support foreign key creation out of create table");
	}
	
}