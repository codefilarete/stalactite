package org.codefilarete.stalactite.engine.idprovider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codefilarete.stalactite.engine.CurrentThreadTransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class PooledSequenceIdentifierProviderTest {
	
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void setUp() {
		persistenceContext = new PersistenceContext(new CurrentThreadTransactionalConnectionProvider(new HSQLDBInMemoryDataSource()), HSQLDBDialectBuilder.defaultHSQLDBDialect());
	}
	
	@Test
	public void giveNewIdentifier() {
		// Creation of an in-memory database pooled sequence generator
		PooledHiLoSequence sequenceIdentifierGenerator = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", PooledHiLoSequenceStorageOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(sequenceIdentifierGenerator.getPersister().getMapping().getTargetTable()));
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
		for (int i = 1; i < 15; i++) {
			long persistableIdentifier = testInstance.giveNewIdentifier();
			generated.add(persistableIdentifier);
			expectedGeneration.add((long) i);
		}
		
		// The generated values must be those expected
		assertThat(generated).isEqualTo(expectedGeneration);
	}
}