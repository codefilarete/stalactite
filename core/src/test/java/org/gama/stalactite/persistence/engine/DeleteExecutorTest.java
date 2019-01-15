package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.PairSetList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.stalactite.test.PairSetList.pairSetList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DeleteExecutorTest extends AbstractDMLExecutorTest {
	
	private DataSet dataSet;
	
	private DeleteExecutor<Toto, Integer, Table> testInstance;
	
	@BeforeEach
	public void setUp() throws SQLException {
		dataSet = new DataSet();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new DeleteExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(dataSet.preparedStatement, times(1)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(0)).executeUpdate();
		verify(dataSet.preparedStatement, times(1)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertEquals("delete from Toto where a = ?", dataSet.statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.setRowCountManager(RowCountManager.NOOP_ROW_COUNT_MANAGER);
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		assertEquals(Arrays.asList("delete from Toto where a = ?"), dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(4)).addBatch();
		verify(dataSet.preparedStatement, times(2)).executeBatch();
		verify(dataSet.preparedStatement, times(0)).executeUpdate();
		verify(dataSet.preparedStatement, times(4)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(1, 2).add(1, 3).newRow(1, 4);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById() throws Exception {
		testInstance.deleteById(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(dataSet.preparedStatement, times(0)).addBatch();
		verify(dataSet.preparedStatement, times(0)).executeBatch();
		verify(dataSet.preparedStatement, times(1)).executeUpdate();
		verify(dataSet.preparedStatement, times(1)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		assertEquals("delete from Toto where a in (?)", dataSet.statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 7);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_multiple_lastBlockContainsOneValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto where a in (?)"), dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(1)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(1)).executeUpdate();
		verify(dataSet.preparedStatement, times(4)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_multiple_lastBlockContainsMultipleValue() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto where a in (?, ?)"), dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(1)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(1)).executeUpdate();
		verify(dataSet.preparedStatement, times(5)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = pairSetList(1, 1).add(2, 2).add(3, 3).newRow(1, 4).add(2, 5);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where a in (?, ?, ?)"), dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(2)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(0)).executeUpdate();
		verify(dataSet.preparedStatement, times(6)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.newRow(1, 1).add(2, 2).add(3, 3)
				.newRow(1, 4).add(2, 5).add(3, 6);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_composedId_multiple_lastBlockContainsOneValue() throws Exception {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", "delete from Toto where (a, b) in ((?, ?))"),
				dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(1)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(1)).executeUpdate();
		verify(dataSet.preparedStatement, times(8)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_composedId_multiple_lastBlockContainsMultipleValue() throws Exception {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))", "delete from Toto where (a, b) in ((?, ?), (?, ?))"),
				dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(1)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(1)).executeUpdate();
		verify(dataSet.preparedStatement, times(10)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43).add(3, 5).add(4, 59);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
	
	@Test
	public void testDeleteById_composedId_multiple_lastBlockSizeIsInOperatorSize() throws Exception {
		DataSetWithComposedId dataSet = new DataSetWithComposedId();
		DMLGenerator dmlGenerator = new DMLGenerator(dataSet.dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		DeleteExecutor<Toto, Toto, Table>testInstance = new DeleteExecutor<>(dataSet.persistenceConfiguration.classMappingStrategy, dataSet.transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
		
		testInstance.deleteById(Arrays.asList(
				new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41),
				new Toto(4, 43, 53), new Toto(5, 59, 61), new Toto(6, 67, 71)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where (a, b) in ((?, ?), (?, ?), (?, ?))"),
				dataSet.statementArgCaptor.getAllValues());
		verify(dataSet.preparedStatement, times(2)).addBatch();
		verify(dataSet.preparedStatement, times(1)).executeBatch();
		verify(dataSet.preparedStatement, times(0)).executeUpdate();
		verify(dataSet.preparedStatement, times(12)).setInt(dataSet.indexCaptor.capture(), dataSet.valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.add(1, 1).add(2, 17).add(3, 2).add(4, 29).add(5, 3).add(6, 37)
				.newRow(1, 4).add(2, 43).add(3, 5).add(4, 59).add(5, 6).add(6, 67);
		assertCapturedPairsEqual(dataSet, expectedPairs);
	}
}