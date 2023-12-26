package org.codefilarete.stalactite.engine.idprovider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.engine.TransactionalConnectionProvider;
import org.codefilarete.stalactite.mapping.id.sequence.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.SequenceStorageOptions;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
public class PooledSequenceIdentifierProviderTest {
	
	private PooledHiLoSequence sequenceIdentifierGenerator;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void setUp() {
		persistenceContext = new PersistenceContext(new TransactionalConnectionProvider(new HSQLDBInMemoryDataSource()), new HSQLDBDialect());
	}
	
	@Test
	public void giveNewIdentifier() {
		// Creation of an in-memory database pooled sequence generator
		sequenceIdentifierGenerator = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT),
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
	
	@Test
	public void giveNewIdentifier_persistenceContextDoesntProvideSeparateTransactionExecutor_throwsException() {
		// we create a PersistenceContext that doesn't have a SeparateTransactionExecutor as connection provider,
		// it will cause error later
		PersistenceContext persistenceContext = new PersistenceContext(new CurrentThreadConnectionProvider(new HSQLDBInMemoryDataSource()),
				new HSQLDBDialect());
		// Creation of an in-memory database pooled sequence generator
		sequenceIdentifierGenerator = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT),
				persistenceContext.getDialect(),
				// whereas PersistenceContext connection provider has no real SeparateTransactionExecutor, it can be cast as such
				(SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(sequenceIdentifierGenerator.getPersister().getMapping().getTargetTable()));
		ddlDeployer.deployDDL();
		
		// Creation of our test instance
		List<Long> starterKit = Arrays.asList();
		Executor sameThreadExecutor = Runnable::run;
		PooledSequenceIdentifierProvider testInstance = new PooledSequenceIdentifierProvider(starterKit, 2, sameThreadExecutor, Duration.ofSeconds(2),
				sequenceIdentifierGenerator);
		assertThatThrownBy(testInstance::giveNewIdentifier)
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Can't execute operation in separate transaction because connection provider doesn't implement o.c.s.e.SeparateTransactionExecutor");
	}
}