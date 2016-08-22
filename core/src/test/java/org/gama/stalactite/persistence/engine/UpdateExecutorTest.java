package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.test.PairSetList;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class UpdateExecutorTest extends DMLExecutorTest {
	
	private UpdateExecutor<Toto, Integer> testInstance;
	
	public void setUpTest() throws SQLException {
		super.setUpTest();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new UpdateExecutor<>(persistenceConfiguration.classMappingStrategy, transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
	}
	
	@Test
	public void testUpdateRoughly() throws Exception {
		testInstance.updateRoughly(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 17).add(2, 23).add(3, 1)
				.of(1, 29).add(2, 31).add(3, 2)
				.of(1, 37).add(2, 41).add(3, 3)
				.of(1, 43).add(2, 53).add(3, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@DataProvider
	public static Object[][] testUpdate_diff_data() {
		return new Object[][] {
				// extreme case: no differences
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
						Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
						new ExpectedResult_TestUpdate_diff(0, 0, 0, Collections.EMPTY_LIST, new PairSetList<Integer, Integer>()) },
				// case: always the same kind of modification: only "b" field
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3)),
						Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3)),
						new ExpectedResult_TestUpdate_diff(3, 1, 6,
								Arrays.asList("update Toto set b = ? where a = ?"),
								new PairSetList<Integer, Integer>().of(1, 2).add(2, 1).add(1, 3).add(2, 2).add(1, 4).add(2, 3)) },
				// case: always the same kind of modification: only "b" field, but batch should be called twice
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
						Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3), new Toto(4, 5, 4)),
						new ExpectedResult_TestUpdate_diff(4, 2, 8,
								Arrays.asList("update Toto set b = ? where a = ?"),
								new PairSetList<Integer, Integer>().of(1, 2).add(2, 1).add(1, 3).add(2, 2).add(1, 4).add(2, 3).add(1, 5).add(2, 4)) },
				// case: always the same kind of modification: "b" + "c" fields
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
						Arrays.asList(new Toto(1, 11, 11), new Toto(2, 22, 22), new Toto(3, 33, 33), new Toto(4, 44, 44)),
						new ExpectedResult_TestUpdate_diff(4, 2, 12,
								Arrays.asList("update Toto set b = ?, c = ? where a = ?"),
								new PairSetList<Integer, Integer>().of(1, 11).add(2, 11).add(3, 1).add(1, 22).add(2, 22).add(3, 2).add(1, 33).add(2, 33).add(3, 3).add(1, 44).add(2, 44).add(3, 4)) },
				// more complex case: mix of modification sort, with batch updates
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4), new Toto(5, 5, 5), new Toto(6, 6, 6), new Toto(7, 7, 7)),
						Arrays.asList(new Toto(1, 11, 1), new Toto(2, 22, 2), new Toto(3, 33, 3), new Toto(4, 44, 444), new Toto(5, 55, 555), new Toto(6, 66, 666), new Toto(7, 7, 7)),
						new ExpectedResult_TestUpdate_diff(6, 2, 15,
								Arrays.asList("update Toto set b = ? where a = ?", "update Toto set b = ?, c = ? where a = ?"),
								new PairSetList<Integer, Integer>().of(1, 11).add(2, 1).add(1, 22).add(2, 2).add(1, 33).add(2, 3)
										.add(1, 44).add(2, 444).add(3, 4)
										.add(1, 55).add(2, 555).add(3, 5)
										.add(1, 66).add(2, 666).add(3, 6)) },
		};
	}
	
	private static class ExpectedResult_TestUpdate_diff {
		private final int addBatchCallCount;
		private final int executeBatchCallCount;
		private final int setIntCallCount;
		private final List<String> updateStatements;
		private final PairSetList<Integer, Integer> statementValues;
		
		public ExpectedResult_TestUpdate_diff(int addBatchCallCount, int executeBatchCallCount, int setIntCallCount, List<String> updateStatements, PairSetList<Integer, Integer> statementValues) {
			this.addBatchCallCount = addBatchCallCount;
			this.executeBatchCallCount = executeBatchCallCount;
			this.setIntCallCount = setIntCallCount;
			this.updateStatements = updateStatements;
			this.statementValues = statementValues;
		}
	}
	
	@Test
	@UseDataProvider("testUpdate_diff_data")
	public void testUpdate_diff(final List<Toto> originalInstances, final List<Toto> modifiedInstances, ExpectedResult_TestUpdate_diff expectedResult) throws Exception {
		testInstance.update(new Iterable<Map.Entry<Toto, Toto>>() {
			@Override
			public Iterator<Map.Entry<Toto, Toto>> iterator() {
				return new PairIterator<>(modifiedInstances, originalInstances);
			}
		}, false);
		
		verify(preparedStatement, times(expectedResult.addBatchCallCount)).addBatch();
		verify(preparedStatement, times(expectedResult.executeBatchCallCount)).executeBatch();
		verify(preparedStatement, times(expectedResult.setIntCallCount)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(expectedResult.updateStatements, statementArgCaptor.getAllValues());
		assertCapturedPairsEqual(expectedResult.statementValues);
	}
	
	@Test
	public void testUpdate_diff_allColumns() throws Exception {
		final List<Toto> originalInstances = Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53), new Toto(5, -1, -2));
		final List<Toto> modifiedInstances = Arrays.asList(new Toto(1, 17, 123), new Toto(2, 129, 31), new Toto(3, 137, 141), new Toto(4, 143, 153), new Toto(5, -1, -2));
		testInstance.update(new Iterable<Map.Entry<Toto, Toto>>() {
			@Override
			public Iterator<Map.Entry<Toto, Toto>> iterator() {
				return new PairIterator<>(modifiedInstances, originalInstances);
			}
		}, true);
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("update Toto set b = ?, c = ? where a = ?"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 17).add(2, 123).add(3, 1)
				.of(1, 129).add(2, 31).add(3, 2)
				.of(1, 137).add(2, 141).add(3, 3)
				.of(1, 143).add(2, 153).add(3, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
}