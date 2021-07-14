package org.gama.stalactite.persistence.id;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.sequence.SequencePersister;
import org.gama.stalactite.persistence.id.sequence.SequenceStorageOptions;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.DDLGenerator;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SequencePersisterTest {
	
	private SequencePersister testInstance;
	private Dialect dialect;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void setUp() {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		dialect = new Dialect(simpleTypeMapping);
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), dialect);
		testInstance = new SequencePersister(dialect, (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	@Test
	public void testGetCreationScripts() {
		DDLGenerator ddlGenerator = new DDLGenerator(dialect.getJavaTypeToSqlTypeMapping());
		ddlGenerator.addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = ddlGenerator.getCreationScripts();
		assertThat(creationScripts).isEqualTo(Arrays.asList("create table sequence_table(sequence_name VARCHAR(255), next_val int, primary key " 
				+ "(sequence_name))"));
	}
	
	@Test
	public void testGetCreationScripts_customized() {
		testInstance = new SequencePersister(new SequenceStorageOptions("myTable", "mySequenceNameCol", "myNextValCol"),
				dialect, (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLGenerator ddlGenerator = new DDLGenerator(dialect.getJavaTypeToSqlTypeMapping());
		ddlGenerator.addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = ddlGenerator.getCreationScripts();
		assertThat(creationScripts).isEqualTo(Arrays.asList("create table myTable(mySequenceNameCol VARCHAR(255), myNextValCol int, primary key " 
				+ "(mySequenceNameCol))"));
	}
	
	@Test
	public void testReservePool_emptyDatabase() throws Exception {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(testInstance.getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		long identifier = testInstance.reservePool("toto", 10);
		assertThat(identifier).isEqualTo(10);
		Connection currentConnection = persistenceContext.getConnectionProvider().getCurrentConnection();
		Statement statement = currentConnection.createStatement();
		ResultSet resultSet = statement.executeQuery("select next_val from sequence_table where sequence_name = 'toto'");
		resultSet.next();
		assertThat(resultSet.getInt("next_val")).isEqualTo(10);
		
		identifier = testInstance.reservePool("toto", 10);
		assertThat(identifier).isEqualTo(20);
		
		resultSet = statement.executeQuery("select next_val from sequence_table where sequence_name = 'toto'");
		resultSet.next();
		assertThat(resultSet.getInt("next_val")).isEqualTo(20);
	}
}