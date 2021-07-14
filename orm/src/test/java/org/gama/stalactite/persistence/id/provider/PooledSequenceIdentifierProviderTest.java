package org.gama.stalactite.persistence.id.provider;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.gama.lang.collection.Arrays;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.sequence.PooledHiLoSequence;
import org.gama.stalactite.persistence.id.sequence.PooledHiLoSequenceOptions;
import org.gama.stalactite.persistence.id.sequence.SequenceStorageOptions;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
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
	private JavaTypeToSqlTypeMapping simpleTypeMapping;
	
	@BeforeEach
	public void setUp() {
		simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping));
	}
	
	@Test
	public void testGiveNewIdentifier() {
		// Creation of an in-memory database pooled sequence generator
		sequenceIdentifierGenerator = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT),
				persistenceContext.getDialect(), (SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(sequenceIdentifierGenerator.getPersister().getMappingStrategy().getTargetTable()));
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
			long persistableIdentifier = testInstance.giveNewIdentifier();
			generated.add(persistableIdentifier);
			expectedGeneration.add((long) i);
		}
		
		// The generated values must be those expected
		assertThat(generated).isEqualTo(expectedGeneration);
	}
	
	@Test
	public void testGiveNewIdentifier_persistenceContextDoesntProvideSeparateTransactionExecutor_throwsException() {
		// we create a PersistenceContext that doesn't have a SeparateTransactionExecutor as connection provider,
		// it will cause error later
		PersistenceContext persistenceContext = new PersistenceContext(new DataSourceConnectionProvider(new HSQLDBInMemoryDataSource()),
				new Dialect(simpleTypeMapping));
		// Creation of an in-memory database pooled sequence generator
		sequenceIdentifierGenerator = new PooledHiLoSequence(new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT),
				persistenceContext.getDialect(),
				// whereas PersistenceContext connection provider has no real SeparateTransactionExecutor, it can be cast as such
				(SeparateTransactionExecutor) persistenceContext.getConnectionProvider(), persistenceContext.getJDBCBatchSize());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(sequenceIdentifierGenerator.getPersister().getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		
		// Creation of our test instance
		List<Long> starterKit = Arrays.asList();
		Executor sameThreadExecutor = Runnable::run;
		PooledSequenceIdentifierProvider testInstance = new PooledSequenceIdentifierProvider(starterKit, 2, sameThreadExecutor, Duration.ofSeconds(2),
				sequenceIdentifierGenerator);
		assertThatThrownBy(testInstance::giveNewIdentifier)
				.extracting(t -> Exceptions.findExceptionInCauses(t, RuntimeException.class), InstanceOfAssertFactories.THROWABLE)
				.hasMessage("Can't execute operation in separate transaction because connection provider doesn't implement o.g.s.p.e.SeparateTransactionExecutor");
	}
}