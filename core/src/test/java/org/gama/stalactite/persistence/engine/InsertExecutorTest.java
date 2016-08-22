package org.gama.stalactite.persistence.engine;

import java.sql.SQLException;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.test.PairSetList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
public class InsertExecutorTest extends DMLExecutorTest {
	
	private InsertExecutor<Toto, Integer> testInstance;
	
	public void setUpTest() throws SQLException {
		super.setUpTest();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new InsertExecutor<>(persistenceConfiguration.classMappingStrategy, transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
	}
	
	@Test
	public void testInsert_simple() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>()
				.of(1, 1).add(2, 17).add(3, 23)
				.of(1, 2).add(2, 29).add(3, 31)
				.of(1, 3).add(2, 37).add(3, 41)
				.of(1, 4).add(2, 43).add(3, 53);
		assertCapturedPairsEqual(expectedPairs);
	}
}