package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.test.PairSetList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
public class SelectExecutorTest extends DMLExecutorTest {
	
	private SelectExecutor<Toto> testInstance;
	
	@BeforeMethod
	public void setUpTest() throws SQLException {
		super.setUpTest();
		DMLGenerator dmlGenerator = new DMLGenerator(dialect.getColumnBinderRegistry(), new DMLGenerator.CaseSensitiveSorter());
		testInstance = new SelectExecutor<>(totoClassMappingStrategy, transactionManager, dmlGenerator, 3);
	}
	
	@Test
	public void testSelect_one() throws Exception {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList((Serializable) 7));
		
		verify(preparedStatement, times(1)).executeQuery();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("select a, b, c from Toto where a in (?)", statementArgCaptor.getValue());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().of(1, 7);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testSelect_multiple() throws Exception {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(Arrays.asList((Serializable) 11, 13, 17, 23));
		
		// two queries because in operator is bounded to 3 values
		verify(preparedStatement, times(2)).executeQuery();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("select a, b, c from Toto where a in (?, ?, ?)", "select a, b, c from Toto where a in (?)"), statementArgCaptor.getAllValues());
		PairSetList<Integer, Integer> expectedPairs = new PairSetList<Integer, Integer>().of(1, 11).add(2, 13).add(3, 17).of(1, 23);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	public void assertCapturedPairsEqual(PairSetList<Integer, Integer> expectedPairs) {
		List<Map.Entry<Integer, Integer>> obtainedPairs = PairSetList.toPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		List<Set<Map.Entry<Integer, Integer>>> obtained = new ArrayList<>();
		int startIndex = 0;
		for (Set<Map.Entry<Integer, Integer>> expectedPair : expectedPairs.asList()) {
			obtained.add(new HashSet<>(obtainedPairs.subList(startIndex, startIndex += expectedPair.size())));
		}
		assertEquals(expectedPairs.asList(), obtained);
	}
	
	@Test
	public void testSelect_hsqldb() throws SQLException {
		final HSQLDBInMemoryDataSource dataSource = new HSQLDBInMemoryDataSource();
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlSchemaGenerator(), transactionManager);
		ddlDeployer.getDdlSchemaGenerator().addTables(totoClassTable);
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		List<Toto> totos = testInstance.select(Arrays.asList((Serializable) 1));
		Toto t = Iterables.first(totos);
		assertEquals(1, (Object) t.a);
		assertEquals(10, (Object) t.b);
		assertEquals(100, (Object) t.c);
		totos = testInstance.select(Arrays.asList((Serializable) 2, 3, 4));
		for (int i = 2; i <= 4; i++) {
			t = totos.get(i - 2);
			assertEquals(i, (Object) t.a);
			assertEquals(10 * i, (Object) t.b);
			assertEquals(100 * i, (Object) t.c);
		}
	}
	
}