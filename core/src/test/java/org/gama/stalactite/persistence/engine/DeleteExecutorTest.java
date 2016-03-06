package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.test.PairSetList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DeleteExecutorTest extends DMLExecutorTest {
	
	private DeleteExecutor<Toto> testInstance;
	
	@BeforeMethod
	public void setUpTest() throws SQLException {
		super.setUpTest();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new DeleteExecutor<>(persistenceConfiguration.classMappingStrategy, transactionManager, dmlGenerator, Retryer.NO_RETRY, 3, 3);
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(7, 17, 23)));
		
		verify(preparedStatement, times(1)).executeUpdate();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete from Toto where a in (?)", statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().of(1, 7);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete from Toto where a in (?, ?, ?)", "delete from Toto where a in (?)"), statementArgCaptor.getAllValues());
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(1)).executeUpdate();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().of(1, 1).add(2, 2).add(3, 3).of(1, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
}