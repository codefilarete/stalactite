package org.codefilarete.stalactite.sql;

import java.sql.SQLException;
import java.util.HashMap;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSelector;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.DDLSequenceGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.stalactite.sql.test.DerbyInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class DerbySequenceSelectBuilderTest {
	
	@Test
	void sqlIsValid() throws SQLException {
		DerbyInMemoryDataSource dataSource = new DerbyInMemoryDataSource();
		SimpleConnectionProvider connectionProvider = new SimpleConnectionProvider(dataSource.getConnection());
		
		// Creating schema
		Sequence sequence = new Sequence("my_sequence");
		DMLNameProvider dmlNameProvider = new DMLNameProvider(new HashMap<>());
		DDLDeployer ddlDeployer = new DDLDeployer(new DDLTableGenerator(new SqlTypeRegistry(), tableAliaser -> dmlNameProvider), new DDLSequenceGenerator(dmlNameProvider), connectionProvider);
		ddlDeployer.getDdlGenerator().addSequences(sequence);
		ddlDeployer.deployDDL();
		
		// testing SQL is valid through Dialect
		DatabaseSequenceSelector sequenceSelector = new DatabaseSequenceSelector(sequence, new DerbyDialect(), connectionProvider);
		// by default HSQLDB sequence starts at -2147483648
		assertThat(sequenceSelector.next()).isEqualTo(-2_147_483_648);
	}
}