package org.gama.stalactite.persistence.id.sequence;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.stalactite.sql.binder.DefaultResultSetReaders;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.sql.result.RowIterator;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.DDLDeployer;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PooledHiLoSequenceTest {
	
	private PooledHiLoSequence testInstance;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void setUp() {
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Long.class, "int");
		simpleTypeMapping.put(String.class, "VARCHAR(255)");
		
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), new Dialect(simpleTypeMapping));
	}
	
	@Test
	public void testGenerate() throws SQLException {
		Dialect dialect = persistenceContext.getDialect();
		SeparateTransactionExecutor connectionProvider = (SeparateTransactionExecutor) persistenceContext.getConnectionProvider();
		int jdbcBatchSize = persistenceContext.getJDBCBatchSize();
		
		// Instanciantion of the sequence, for schema generation
		PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT);
		testInstance = new PooledHiLoSequence(totoSequenceOptions, dialect, connectionProvider, jdbcBatchSize);
		
		// Generating sequence table schema
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(testInstance.getPersister().getMappingStrategy().getTargetTable()));
		ddlDeployer.deployDDL();
		
		// we check that we can increment from an empty database
		for (int i = 0; i < 45; i++) {
			assertEquals(i, testInstance.next().intValue());
		}
		
		SequenceStorageOptions sequenceStorageOptions = totoSequenceOptions.getStorageOptions();
		PreparedStatement sequenceValueReader = connectionProvider.getCurrentConnection().prepareStatement(
				"select " + sequenceStorageOptions.getValueColumn()
						+ " from " + sequenceStorageOptions.getTable()
						+ " where " + sequenceStorageOptions.getSequenceNameColumn() + " = ?");
		sequenceValueReader.setString(1, totoSequenceOptions.getSequenceName());
		RowIterator sequenceValues = new RowIterator(sequenceValueReader.executeQuery(), Maps.asMap(sequenceStorageOptions.getValueColumn(), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER));
		sequenceValues.hasNext();
		Row row = sequenceValues.next();
		assertEquals(50, row.get(sequenceStorageOptions.getValueColumn()));
		
		// we check that we can increment from a database with a new sequence in the same table
		PooledHiLoSequenceOptions tataSequenceOptions = new PooledHiLoSequenceOptions(10, "Tata", SequenceStorageOptions.DEFAULT);
		testInstance = new PooledHiLoSequence(tataSequenceOptions, dialect, connectionProvider, jdbcBatchSize);
		for (int i = 0; i < 45; i++) {
			assertEquals(i, testInstance.next().intValue());
		}
		
		// we check that we can increment from a database with an existing sequence
		testInstance = new PooledHiLoSequence(totoSequenceOptions, dialect, connectionProvider, jdbcBatchSize);
		assertEquals(50, testInstance.next().intValue());
		// after first access, sequence must be incremented
		sequenceValueReader.setString(1, totoSequenceOptions.getSequenceName());
		sequenceValues = new RowIterator(sequenceValueReader.executeQuery(), Maps.asMap(sequenceStorageOptions.getValueColumn(), DefaultResultSetReaders.INTEGER_PRIMITIVE_READER));
		sequenceValues.hasNext();
		row = sequenceValues.next();
		// previous call ends at 50, so first increment mus put it to 50
		assertEquals(60, row.get(sequenceStorageOptions.getValueColumn()));

		for (int i = 1; i < 45; i++) {
			// 50 because previous call to Toto sequence had a pool size of 10, and its last call was 45. So the external state was 50.
			// (upper bound of 45 by step of 10)
			assertEquals(50+i, testInstance.next().intValue());
		}
		
		// we check that we can increment a sequence from a different initial value
		PooledHiLoSequenceOptions titiSequenceOptions = new PooledHiLoSequenceOptions(10, "Titi", SequenceStorageOptions.DEFAULT, -42);
		testInstance = new PooledHiLoSequence(titiSequenceOptions, dialect, connectionProvider, jdbcBatchSize);
		for (int i = -42; i < -25; i++) {
			assertEquals(i, testInstance.next().intValue());
		}
	}
}