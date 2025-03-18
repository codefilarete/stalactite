package org.codefilarete.stalactite.sql.ddl;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;

import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.OracleDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.test.OracleDatabaseHelper;
import org.codefilarete.stalactite.sql.test.OracleEmbeddableDataSource;
import org.junit.jupiter.api.Test;

class OracleDDLTableGeneratorTest extends DDLTableGeneratorTest.IntegrationTest {
	
	/**
	 * Overridden to take Oracle particularities :
	 * - an index can't be added on a column that already has a unique constraint
	 * - a foreign key can't reference anything else than a primary key or a unique column
	 */
	@Override
	protected void defineSchema() {
		// testing primary key and auto-increment
		table1.addColumn("id", int.class).primaryKey().autoGenerated();
		Column<?, String> nameColumn = table1.addColumn("name", String.class);
		// testing unique constraint creation
		table1.addUniqueConstraint("dummy_UK", nameColumn);
		// testing index creation
		Column<?, Long> numberColumn = table1.addColumn("version", long.class);
		table1.addIndex("dummyIDX_1", numberColumn);
		
		Column nameColumn2 = table2.addColumn("name", String.class);
		// testing foreign key constraint creation
		table2.addUniqueConstraint("dummy_UK2", nameColumn2);
		table2.addForeignKey("dummyTable2_FK", nameColumn2, nameColumn);
	}
	
	@Test
	void generatedSQL_runOnAliveDatabase_doesNotThrowException() throws SQLException {
		DataSource OracleDataSource = new OracleEmbeddableDataSource();
		Dialect OracleDialect = OracleDialectBuilder.defaultOracleDialect();
		OracleDDLTableGenerator testInstance = (OracleDDLTableGenerator) OracleDialect.getDdlTableGenerator();
		assertGeneratedSQL_runOnAliveDatabase_doesNotThrowException(testInstance, OracleDataSource.getConnection());
		
		// Due to foreign key between tables, we must clean the schema by giving the right table order
		// because Oracle doesn't support dropping a table when it's referenced by another one
		// This only cleans up schema for next tests and overwrites the job done by DatabaseIntegrationTest.clearDatabaseSchema() 
		new OracleDatabaseHelper().dropTable(OracleDataSource.getConnection(), Arrays.asList("DUMMYTABLE2", "DUMMYTABLE1").iterator());
	}
}