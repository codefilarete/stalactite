package org.codefilarete.stalactite.sql;

import java.sql.SQLException;

import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class HSQLDBSequenceSelectBuilderTest {
	
	@Test
	void sqlIsValid() throws SQLException {
		HSQLDBInMemoryDataSource dataSource = new HSQLDBInMemoryDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		
		// Creating schema
		Sequence sequence = new Sequence("my_sequence");
		HSQLDBDialect dialect = new HSQLDBDialect();
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator(), connectionProvider);
		ddlDeployer.getDdlGenerator().addSequences(sequence);
		ddlDeployer.deployDDL();
		
		// testing SQL is valid through Dialect
		org.codefilarete.tool.function.Sequence<Long> sequenceSelector = dialect.getDatabaseSequenceSelectorFactory().create(sequence, connectionProvider);
		// by default HSQLDB sequence starts at 0
		assertThat(sequenceSelector.next()).isEqualTo(0);
	}
}