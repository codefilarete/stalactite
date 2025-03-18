package org.codefilarete.stalactite.sql;

import java.sql.SQLException;

import org.codefilarete.stalactite.mapping.id.sequence.SequenceStoredAsTableSelector;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.test.SQLiteInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class SQLiteSequenceSelectBuilderTest {
	
	@Test
	void sqlIsValid() throws SQLException {
		SQLiteInMemoryDataSource dataSource = new SQLiteInMemoryDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		
		// Creating schema
		Sequence sequence = new Sequence("my_sequence");
		Dialect dialect = SQLiteDialectBuilder.defaultSQLiteDialect();
		
		// testing SQL is valid through Dialect
		org.codefilarete.tool.function.Sequence sequenceSelector = dialect.getDatabaseSequenceSelectorFactory().create(sequence, connectionProvider);
		
		// deploying schema by taking Table from fake database sequence
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), connectionProvider);
		Table sequenceMimic = ((SequenceStoredAsTableSelector) sequenceSelector).getSequenceTable();
		ddlDeployer.getDdlGenerator().addTables(sequenceMimic);
		ddlDeployer.deployDDL();
		
		
		
		// by default SQLite sequence starts at 0
		assertThat(sequenceSelector.next()).isEqualTo(1L);
		assertThat(sequenceSelector.next()).isEqualTo(2L);
	}
}