package org.gama.stalactite.persistence.id.sequence;

import java.io.Serializable;
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

public class PooledSequenceIdentifierGeneratorTest {
	
	private PooledSequenceIdentifierGenerator testInstance;
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
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Toto", PooledSequencePersistenceOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(testInstance.getPooledSequencePersister().getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		// on vérifie que l'incrémentation se fait sans erreur depuis une base vierge
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals((long) i, newId);
		}
		
		// on vérifie que l'incrémentation se fait sans erreur avec une nouvelle sequence sur la même table
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Tata", PooledSequencePersistenceOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals((long) i, newId);
		}
		
		// on vérifie que l'incrémentation se fait sans erreur avec sequence existante
		testInstance = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Toto", PooledSequencePersistenceOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		for (int i = 0; i < 45; i++) {
			Serializable newId = testInstance.generate();
			assertEquals((long) 50+i, newId);
		}
	}
}