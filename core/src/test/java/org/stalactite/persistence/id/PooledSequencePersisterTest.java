package org.stalactite.persistence.id;

import static org.testng.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.stalactite.lang.collection.Arrays;
import org.stalactite.persistence.engine.DDLDeployer;
import org.stalactite.persistence.engine.PersistenceContext;
import org.stalactite.persistence.id.sequence.PooledSequencePersistenceOptions;
import org.stalactite.persistence.id.sequence.PooledSequencePersister;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.test.HSQLDBInMemoryDataSource;
import org.stalactite.test.JdbcTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PooledSequencePersisterTest {
	
	private PooledSequencePersister testInstance;
	
	@BeforeMethod
	public void setUp() {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		PersistenceContext.setCurrent(new PersistenceContext(new JdbcTransactionManager(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping)));
		testInstance = new PooledSequencePersister();
	}
	
	@AfterMethod
	public void tearDown() {
		PersistenceContext.clearCurrent();
	}
	
	@Test
	public void testGetCreationScripts() throws Exception {
		List<String> creationScripts = testInstance.getCreationScripts();
		assertEquals(creationScripts, Arrays.asList("create table sequence_table(sequence_name VARCHAR(255), next_val int, primary key (sequence_name))"));
	}
	
	@Test
	public void testGetCreationScripts_customized() throws Exception {
		testInstance = new PooledSequencePersister(new PooledSequencePersistenceOptions("myTable", "mySequenceNameCol", "myNextValCol"));
		List<String> creationScripts = testInstance.getCreationScripts();
		assertEquals(creationScripts, Arrays.asList("create table myTable(mySequenceNameCol VARCHAR(255), myNextValCol int, primary key (mySequenceNameCol))"));
	}
	
	@Test
	public void testReservePool_emptyDatabase() throws Exception {
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance.getPersistenceContext());
		ddlDeployer.deployDDL();
		long identifier = testInstance.reservePool("toto", 10);
		assertEquals(identifier, 10);
		Connection currentConnection = PersistenceContext.getCurrent().getCurrentConnection();
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