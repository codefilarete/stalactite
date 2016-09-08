package org.gama.stalactite.persistence.id;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.sequence.SequencePersisterOptions;
import org.gama.stalactite.persistence.id.sequence.SequencePersister;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SequencePersisterTest {
	
	private SequencePersister testInstance;
	private Dialect dialect;
	private PersistenceContext persistenceContext;
	
	@Before
	public void setUp() {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		dialect = new Dialect(simpleTypeMapping);
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), dialect);
		testInstance = new SequencePersister(dialect, (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
	}
	
	@Test
	public void testGetCreationScripts() throws Exception {
		dialect.getDdlSchemaGenerator().addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = dialect.getDdlSchemaGenerator().getCreationScripts();
		assertEquals(Arrays.asList("create table sequence_table(sequence_name VARCHAR(255), next_val int, primary key (sequence_name))"), creationScripts);
	}
	
	@Test
	public void testGetCreationScripts_customized() throws Exception {
		testInstance = new SequencePersister(new SequencePersisterOptions("myTable", "mySequenceNameCol", "myNextValCol"),
				dialect, (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		dialect.getDdlSchemaGenerator().addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = dialect.getDdlSchemaGenerator().getCreationScripts();
		assertEquals(Arrays.asList("create table myTable(mySequenceNameCol VARCHAR(255), myNextValCol int, primary key (mySequenceNameCol))"), creationScripts);
	}
	
	@Test
	public void testReservePool_emptyDatabase() throws Exception {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(testInstance.getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		long identifier = testInstance.reservePool("toto", 10);
		assertEquals(identifier, 10);
		Connection currentConnection = persistenceContext.getCurrentConnection();
		Statement statement = currentConnection.createStatement();
		ResultSet resultSet = statement.executeQuery("select next_val from sequence_table where sequence_name = 'toto'");
		resultSet.next();
		assertEquals(10, resultSet.getInt("next_val"));
		
		identifier = testInstance.reservePool("toto", 10);
		assertEquals(20, identifier);
		
		resultSet = statement.executeQuery("select next_val from sequence_table where sequence_name = 'toto'");
		resultSet.next();
		assertEquals(20, resultSet.getInt("next_val"));
	}
}