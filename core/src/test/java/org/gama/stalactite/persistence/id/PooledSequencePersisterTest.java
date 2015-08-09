package org.gama.stalactite.persistence.id;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.id.sequence.PooledSequencePersistenceOptions;
import org.gama.stalactite.persistence.id.sequence.PooledSequencePersister;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class PooledSequencePersisterTest {
	
	private PooledSequencePersister testInstance;
	private Dialect dialect;
	private PersistenceContext persistenceContext;
	
	@BeforeMethod
	public void setUp() {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		dialect = new Dialect(simpleTypeMapping);
		persistenceContext = new PersistenceContext(new JdbcTransactionManager(new HSQLDBInMemoryDataSource()), dialect);
		testInstance = new PooledSequencePersister(dialect, persistenceContext.getTransactionManager(), persistenceContext.getJDBCBatchSize());
	}
	
	@AfterMethod
	public void tearDown() {
		PersistenceContext.clearCurrent();
	}
	
	@Test
	public void testGetCreationScripts() throws Exception {
		dialect.getDdlSchemaGenerator().addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = dialect.getDdlSchemaGenerator().getCreationScripts();
		assertEquals(creationScripts, Arrays.asList("create table sequence_table(sequence_name VARCHAR(255), next_val int, primary key (sequence_name))"));
	}
	
	@Test
	public void testGetCreationScripts_customized() throws Exception {
		testInstance = new PooledSequencePersister(new PooledSequencePersistenceOptions("myTable", "mySequenceNameCol", "myNextValCol"),
				dialect, persistenceContext.getTransactionManager(), persistenceContext.getJDBCBatchSize());
		dialect.getDdlSchemaGenerator().addTables(testInstance.getMappingStrategy().getTargetTable());
		List<String> creationScripts = dialect.getDdlSchemaGenerator().getCreationScripts();
		assertEquals(creationScripts, Arrays.asList("create table myTable(mySequenceNameCol VARCHAR(255), myNextValCol int, primary key (mySequenceNameCol))"));
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
		assertEquals(resultSet.getInt("next_val"), 10);
		
		identifier = testInstance.reservePool("toto", 10);
		assertEquals(identifier, 20);
		
		resultSet = statement.executeQuery("select next_val from sequence_table where sequence_name = 'toto'");
		resultSet.next();
		assertEquals(resultSet.getInt("next_val"), 20);
	}
}