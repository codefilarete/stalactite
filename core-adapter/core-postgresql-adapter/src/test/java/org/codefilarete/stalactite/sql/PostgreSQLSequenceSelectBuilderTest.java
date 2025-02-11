package org.codefilarete.stalactite.sql;

import java.sql.SQLException;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.test.PostgreSQLEmbeddedDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class PostgreSQLSequenceSelectBuilderTest {
	
	@Test
	void sqlIsValid() throws SQLException {
		PostgreSQLEmbeddedDataSource dataSource = new PostgreSQLEmbeddedDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		
		// Creating schema
		Sequence sequence = new Sequence("my_sequence");
		PostgreSQLDialect dialect = new PostgreSQLDialect();
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), connectionProvider);
		ddlDeployer.getDdlGenerator().addSequences(sequence);
		ddlDeployer.deployDDL();
		
		// testing SQL is valid through Dialect
		DatabaseSequenceSelector sequenceSelector = new DatabaseSequenceSelector(sequence, dialect, connectionProvider);
		// by default PostgreSQL sequence starts at 1
		assertThat(sequenceSelector.next()).isEqualTo(1);
	}
}