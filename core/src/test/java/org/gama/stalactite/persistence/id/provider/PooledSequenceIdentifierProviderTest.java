package org.gama.stalactite.persistence.id.provider;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGeneratorOptions;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequencePersistenceOptions;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class PooledSequenceIdentifierProviderTest {
	
	private PooledSequenceIdentifierGenerator sequenceIdentifierGenerator;
	private PersistenceContext persistenceContext;
	
	@Before
	public void setUp() throws SQLException {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping));
	}
	
	@Test
	public void testGiveNewIdentifier() throws SQLException {
		// Creation of an in-memory database pooled sequence generator
		sequenceIdentifierGenerator = new PooledSequenceIdentifierGenerator(new PooledSequenceIdentifierGeneratorOptions(10, "Toto", PooledSequencePersistenceOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(sequenceIdentifierGenerator.getPooledSequencePersister().getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		
		// Creation of our test instance
		List<Long> starterKit = Arrays.asList();
		ExecutorService backgroundLoader = Executors.newSingleThreadExecutor();
		PooledSequenceIdentifierProvider testInstance = new PooledSequenceIdentifierProvider(starterKit, 2, backgroundLoader, Duration.ofSeconds(2),
				sequenceIdentifierGenerator);
		
		// Test
		List<Long> generated = new ArrayList<>();
		List<Long> expectedGeneration = new ArrayList<>();
		// we start at 0 since the sequence will do so since the database is empty
		for (int i = 0; i < 15; i++) {
			PersistableIdentifier<Long> persistableIdentifier = testInstance.giveNewIdentifier();
			generated.add(persistableIdentifier.getSurrogate());
			expectedGeneration.add((long) i);
		}
		
		// The generated values must be those expected
		assertEquals(expectedGeneration, generated);
	}
	
	// TODO: make PooledSequenceIdentifierGenerator be generic
	// TODO: test Identifier with PersistenceContext: must implement the "embeddable" feature
	
}