package org.codefilarete.stalactite.sql.mysql;

import java.sql.SQLException;

import org.codefilarete.stalactite.mapping.id.sequence.SequenceStoredAsTableSelector;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.SimpleConnectionProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.mysql.test.MySQLEmbeddableDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class MySQLSequenceSelectBuilderTest {
	
	@Test
	void sqlIsValid() throws SQLException {
		MySQLEmbeddableDataSource dataSource = new MySQLEmbeddableDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		
		// Creating schema
		Sequence sequence = new Sequence("my_sequence");
		Dialect dialect = MySQLDialectBuilder.defaultMySQLDialect();
		
		// testing SQL is valid through Dialect
		org.codefilarete.tool.function.Sequence<Long> sequenceSelector = dialect.getDatabaseSequenceSelectorFactory().create(sequence, connectionProvider);
		
		// deploying schema by taking Table from fake database sequence
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), connectionProvider);
		Table sequenceMimic = ((SequenceStoredAsTableSelector) sequenceSelector).getSequenceTable();
		ddlDeployer.getDdlGenerator().addTables(sequenceMimic);
		ddlDeployer.deployDDL();
		
		// by default MySQL sequence starts at 1
		assertThat(sequenceSelector.next()).isEqualTo(1);
		assertThat(sequenceSelector.next()).isEqualTo(2);
	}
}
