package org.codefilarete.stalactite.engine.runtime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.engine.StaleStateObjectException;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.SQLOperation.SQLOperationListener;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.test.PairSetList;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.test.PairSetList.pairSetList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DeleteExecutorTest<T extends Table<T>> extends AbstractDMLExecutorMockTest {
	
	private final Dialect dialect = new DefaultDialect(new JavaTypeToSqlTypeMapping()
		.with(Integer.class, "int"));
	
	private DeleteExecutor<Toto, Integer, T> testInstance;
	
	@BeforeEach
	void setUp() {
		PersistenceConfiguration<Toto, Integer, T> persistenceConfiguration = giveDefaultPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		testInstance = new DeleteExecutor<>(persistenceConfiguration.entityMapping,
											new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
	}
	
	@Test
	void delete() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(1)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("delete from Toto where a = ?");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void listenerIsCalled() {
		SQLOperationListener<Column<?, Object>> listenerMock = mock(SQLOperationListener.class);
		testInstance.setOperationListener((SQLOperationListener) listenerMock);
		
		ArgumentCaptor<Map<Column<?, Object>, ?>> statementArgCaptor = ArgumentCaptor.forClass(Map.class);
		ArgumentCaptor<SQLStatement<Column<?, Object>>> sqlArgCaptor = ArgumentCaptor.forClass(SQLStatement.class);
		
		try {
			testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31)));
		} catch (StaleStateObjectException e) {
			// we don't care about any existing data in the database, listener must be called, so we continue even if there are some stale objects
		}
		
		Table mappedTable = new Table("Toto");
		Column colA = mappedTable.addColumn("a", Integer.class);
		Column colB = mappedTable.addColumn("b", Integer.class);
		Column colC = mappedTable.addColumn("c", Integer.class);
		verify(listenerMock, times(2)).onValuesSet(statementArgCaptor.capture());
		assertThat(statementArgCaptor.getAllValues())
				// since Query contains columns copies we can't compare them through equals() (and since Column doesn't implement equals()/hashCode()
				.usingElementComparator(Comparator.comparing(Object::toString))
				.containsExactly(
				Maps.asHashMap(colA, 1),
				Maps.asHashMap(colA, 2)
		);
		verify(listenerMock, times(1)).onExecute(sqlArgCaptor.capture());
		assertThat(sqlArgCaptor.getValue().getSQL()).isEqualTo("delete from Toto where a = ?");
	}
	
	@Test
	void delete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a = ?"));
		verify(jdbcMock.preparedStatement, times(4)).addBatch();
		verify(jdbcMock.preparedStatement, times(2)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(4)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(1, 2).add(1, 3).newRow(1, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById() throws Exception {
		testInstance.deleteById(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(jdbcMock.preparedStatement, times(0)).addBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(1)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		assertThat(jdbcMock.sqlCaptor.getValue()).isEqualTo("delete from Toto where a in (?)");
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockContainsOneValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto " 
				+ "where a in (?)"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(4)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockContainsMultipleValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto " 
				+ "where a in (?, ?)"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(5)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4).add(2, 5);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where a in (?, ?, ?)"));
		verify(jdbcMock.preparedStatement, times(2)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(6)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 1).add(2, 2).add(3, 3)
				.newRow(1, 4).add(2, 5).add(3, 6);
		assertCapturedPairsEqual(jdbcMock, expectedPairs);
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockContainsOneValue() throws Exception {
		PersistenceConfiguration<Toto, Toto, ?> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		DeleteExecutor<Toto, Toto, ?> testInstance = new DeleteExecutor<>(persistenceConfiguration.entityMapping,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", 
				"delete from Toto where (a, b) in ((?, ?))"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(8)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		
		List<Duo<Integer, Integer>> actualValuePairs = arrangeValues(jdbcMock.indexCaptor.getAllValues(), jdbcMock.valueCaptor.getAllValues(), testInstance.getBatchSize());
		assertThat(actualValuePairs).containsExactlyInAnyOrder(new Duo<>(1, 17),
															   new Duo<>(2, 29),
															   new Duo<>(3, 37),
															   new Duo<>(4, 43));
		
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockContainsMultipleValue() throws Exception {
		PersistenceConfiguration<Toto, Toto, ?> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		DeleteExecutor<Toto, Toto, ?> testInstance = new DeleteExecutor<>(persistenceConfiguration.entityMapping,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", 
				"delete from Toto where (a, b) in ((?, ?), (?, ?))"));
		verify(jdbcMock.preparedStatement, times(1)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(10)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		
		System.out.println(jdbcMock.indexCaptor.getAllValues());
		System.out.println(jdbcMock.valueCaptor.getAllValues());
		List<Duo<Integer, Integer>> actualValuePairs = arrangeValues(jdbcMock.indexCaptor.getAllValues(), jdbcMock.valueCaptor.getAllValues(), testInstance.getBatchSize());
		System.out.println("actualValuePairs : " + actualValuePairs);
		assertThat(actualValuePairs).containsExactlyInAnyOrder(new Duo<>(1, 17),
															   new Duo<>(2, 29),
															   new Duo<>(3, 37),
															   new Duo<>(4, 43),
															   new Duo<>(5, 59));
		
	}
	
	@Test
	void deleteById_composedId_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		PersistenceConfiguration<Toto, Toto, ?> persistenceConfiguration = giveIdAsItselfPersistenceConfiguration();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter(), DMLNameProvider::new);
		DeleteExecutor<Toto, Toto, ?>testInstance = new DeleteExecutor<>(persistenceConfiguration.entityMapping,
				new ConnectionConfigurationSupport(jdbcMock.transactionManager, 3), dmlGenerator, noRowCountCheckWriteOperationFactory, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertThat(jdbcMock.sqlCaptor.getAllValues()).isEqualTo(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"));
		verify(jdbcMock.preparedStatement, times(2)).addBatch();
		verify(jdbcMock.preparedStatement, times(1)).executeLargeBatch();
		verify(jdbcMock.preparedStatement, times(0)).executeLargeUpdate();
		verify(jdbcMock.preparedStatement, times(12)).setInt(jdbcMock.indexCaptor.capture(), jdbcMock.valueCaptor.capture());
		
		List<Duo<Integer, Integer>> actualValuePairs = arrangeValues(jdbcMock.indexCaptor.getAllValues(), jdbcMock.valueCaptor.getAllValues(), testInstance.getBatchSize());
		assertThat(actualValuePairs).containsExactlyInAnyOrder(new Duo<>(1, 17),
															   new Duo<>(2, 29),
															   new Duo<>(3, 37),
															   new Duo<>(4, 43),
															   new Duo<>(5, 59),
															   new Duo<>(6, 67));
	}
	
	static List<Duo<Integer, Integer>> arrangeValues(List<Integer> capturedIndexes, List<Integer> capturedValues, int batchSize) {
		// Captured values is a list of values such as :
		// - col A values for block #1 (3 values),
		// - col B values for block #1 (3 values),
		// - col A values for block #2 (3 values),
		// - col B values for block #2 (3 values),
		// - col A values for block #3 (2 values),
		// - col B values for block #3 (2 values)
		// the algorithm below unpacks it to get pairs of (colA, colB)
		Duo<Integer, Integer>[] result = new Duo[capturedIndexes.size() / 2];
		for (int i = 0; i < result.length; i++) {
			result[i] = new Duo<>();
		}
		
		// [1, 3, 5, 2, 4, 6,      1, 3, 2, 4]
		// [4, 2, 1, 43, 29, 17,      3, 5, 37, 59]
		// {1, 17}, {2, 29}, {3, 37}, {4, 43}, {5, 59}
		
		// computing blocks sizes
		int colCount = 2;
		for (int i = 0; i < capturedIndexes.size(); i++) {
			int index = capturedIndexes.get(i);
			int value = capturedValues.get(i);
			int currentBlockNumber = batchSize * (i / (batchSize * colCount));
			int duoNumber = (int) Math.ceil((double) index / colCount) + currentBlockNumber;
			if (index % 2 == 0) {
				result[duoNumber-1].setRight(value);
			} else {
				result[duoNumber-1].setLeft(value);
			}
		};
		return Arrays.asList(result);
	}
}