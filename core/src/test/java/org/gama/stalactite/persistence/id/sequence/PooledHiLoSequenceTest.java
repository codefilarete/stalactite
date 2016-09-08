package org.gama.stalactite.persistence.id.sequence;

import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PooledHiLoSequenceTest {
	
	private PooledHiLoSequence testInstance;
	private PersistenceContext persistenceContext;
	
	@Before
	public void setUp() throws SQLException {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping));
	}
	
	@Test
	public void testGenerate() throws SQLException {
		testInstance = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequencePersisterOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(testInstance.getSequencePersister().getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		// we check that we can increment from an empty database
		for (int i = 0; i < 45; i++) {
			Long newId = testInstance.next();
			assertEquals(i, newId.intValue());
		}
		
		// we check that we can increment from a database with a new sequence in the same table
		testInstance = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Tata", SequencePersisterOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		for (int i = 0; i < 45; i++) {
			Long newId = testInstance.next();
			assertEquals(i, newId.intValue());
		}
		
		// we check that we can increment from a database with an existing sequence
		testInstance = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequencePersisterOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		for (int i = 0; i < 45; i++) {
			Long newId = testInstance.next();
			assertEquals(50+i, newId.intValue());
		}
	}
}