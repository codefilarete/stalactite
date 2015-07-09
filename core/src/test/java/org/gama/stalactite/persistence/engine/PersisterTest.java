package org.gama.stalactite.persistence.engine;

import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.lang.collection.PairIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.test.JdbcTransactionManager;
import org.gama.stalactite.test.PairSet;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class PersisterTest {
	
	public static final String TEST_UPDATE_DIFF_DATA = "testUpdate_diff";
	private Persister<Toto> testInstance;
	private PersistenceContext persistenceContext;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> statementArgCaptor;
	private JdbcTransactionManager transactionManager;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	
	@BeforeTest
	public void setUp() throws SQLException {
		Table totoClassTable = new Table(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		ClassMappingStrategy<Toto> totoClassMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoClassTable,
				totoClassMapping, persistentFieldHarverster.getField("a"), identifierGenerator);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
		
		transactionManager = new JdbcTransactionManager(null);
		persistenceContext = new PersistenceContext(transactionManager, new Dialect(simpleTypeMapping));
		persistenceContext.setJDBCBatchSize(3);
		persistenceContext.add(totoClassMappingStrategy);
	}
	
	@BeforeMethod
	public void setUpTest() throws SQLException {
		// reset id counter between 2 tests else id "overflow"
		identifierGenerator.idCounter.set(0);
		
		preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
		
		Connection connection = mock(Connection.class);
		// PreparedStatement.getConnection() must gives that instance of connection because of SQLOperation that checks
		// weither or not it should prepare statement
		when(preparedStatement.getConnection()).thenReturn(connection);
		statementArgCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
		
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		transactionManager.setDataSource(dataSource);
		testInstance = new Persister<>(persistenceContext, persistenceContext.getMappingStrategy(Toto.class), new DMLGenerator.CaseSensitiveSorter(),
				Retryer.NO_RETRY, 3);
		PersistenceContext.setCurrent(persistenceContext);
	}
	
	@Test
	public void testPersist_insert() throws Exception {
		testInstance.persist(new Toto(17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.
				of(1, 1).add(2, 17).add(3, 23);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testInsert_multiple() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.
				of(1, 1).add(2, 17).add(3, 23)
				.add(1, 2).add(2, 29).add(3, 31)
				.add(1, 3).add(2, 37).add(3, 41)
				.add(1, 4).add(2, 43).add(3, 53);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testPersist_update() throws Exception {
		testInstance.persist(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.
				of(1, 17).add(2, 23).add(3, 7);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testUpdateRoughly_multiple() throws Exception {
		testInstance.updateRoughly(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.
				of(1, 17).add(2, 23).add(3, 1)
				.add(1, 29).add(2, 31).add(3, 2)
				.add(1, 37).add(2, 41).add(3, 3)
				.add(1, 43).add(2, 53).add(3, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@DataProvider(name = TEST_UPDATE_DIFF_DATA)
	public Object[][] testUpdate_diff_data() {
		return new Object[][] {
				// extreme case: no differences
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)), 
					Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
					new ExpectedResult_TestUpdate_diff(0, 0, 0, Collections.EMPTY_LIST, new PairSet<Integer, Integer>()) },
				// case: always the same kind of modification: only "b" field
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3)), 
					Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3)),
					new ExpectedResult_TestUpdate_diff(3, 1, 6,
							Arrays.asList("update Toto set b = ? where a = ?"),
							PairSet.of(1, 2).add(2, 1).add(1, 3).add(2, 2).add(1, 4).add(2, 3)) },
				// case: always the same kind of modification: only "b" field, but batch should be called twice
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
					Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3), new Toto(4, 5, 4)),
					new ExpectedResult_TestUpdate_diff(4, 2, 8,
							Arrays.asList("update Toto set b = ? where a = ?"),
							PairSet.of(1, 2).add(2, 1).add(1, 3).add(2, 2).add(1, 4).add(2, 3).add(1, 5).add(2, 4)) },
				// case: always the same kind of modification: "b" + "c" fields
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
					Arrays.asList(new Toto(1, 11, 11), new Toto(2, 22, 22), new Toto(3, 33, 33), new Toto(4, 44, 44)),
					new ExpectedResult_TestUpdate_diff(4, 2, 12,
							Arrays.asList("update Toto set b = ?, c = ? where a = ?"),
							PairSet.of(1, 11).add(2, 11).add(3, 1).add(1, 22).add(2, 22).add(3, 2).add(1, 33).add(2, 33).add(3, 3).add(1, 44).add(2, 44).add(3, 4)) },
				// more complex case: mix of modification sort, with batch updates
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4), new Toto(5, 5, 5), new Toto(6, 6, 6), new Toto(7, 7, 7)),
					Arrays.asList(new Toto(1, 11, 1), new Toto(2, 22, 2), new Toto(3, 33, 3), new Toto(4, 44, 444), new Toto(5, 55, 555), new Toto(6, 66, 666), new Toto(7, 7, 7)),
					new ExpectedResult_TestUpdate_diff(6, 2, 15,
							Arrays.asList("update Toto set b = ? where a = ?", "update Toto set b = ?, c = ? where a = ?"),
							PairSet.of(1, 11).add(2, 1).add(1, 22).add(2, 2).add(1, 33).add(2, 3)
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
		private final PairSet<Integer, Integer> statementValues;
		
		public ExpectedResult_TestUpdate_diff(int addBatchCallCount, int executeBatchCallCount, int setIntCallCount, List<String> updateStatements, PairSet<Integer, Integer> statementValues) {
			this.addBatchCallCount = addBatchCallCount;
			this.executeBatchCallCount = executeBatchCallCount;
			this.setIntCallCount = setIntCallCount;
			this.updateStatements = updateStatements;
			this.statementValues = statementValues;
		}
	}
	
	@Test(dataProvider = TEST_UPDATE_DIFF_DATA)
	public void testUpdate_diff(final List<Toto> originalInstances, final List<Toto> modifiedInstances, ExpectedResult_TestUpdate_diff expectedResult) throws Exception {
		testInstance.update(new Iterable<Entry<Toto, Toto>>() {
			@Override
			public Iterator<Entry<Toto, Toto>> iterator() {
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
		testInstance.update(new Iterable<Entry<Toto, Toto>>() {
			@Override
			public Iterator<Entry<Toto, Toto>> iterator() {
				return new PairIterator<>(modifiedInstances, originalInstances);
			}
		}, true);
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList("update Toto set b = ?, c = ? where a = ?"), statementArgCaptor.getAllValues());
		PairSet<Integer, Integer> expectedPairs = PairSet.
				of(1, 17).add(2, 123).add(3, 1)
				.add(1, 129).add(2, 31).add(3, 2)
				.add(1, 137).add(2, 141).add(3, 3)
				.add(1, 143).add(2, 153).add(3, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).executeUpdate();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a in (?)", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.of(1, 7);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		// 2 statements because in operator is bounded to 3 values (see testInstance creation)
		assertEquals(Arrays.asList("delete Toto where a in (?, ?, ?)", "delete Toto where a in (?)"), statementArgCaptor.getAllValues());
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(1)).executeUpdate();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		PairSet<Integer, Integer> expectedPairs = PairSet.of(1, 1).add(2, 2).add(3, 3).add(1, 4);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	
	@Test
	public void testSelect_one() throws Exception {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		
		testInstance.select(7);
		
		verify(preparedStatement, times(1)).executeQuery();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("select a, b, c from Toto where a in (?)", statementArgCaptor.getValue());
		PairSet<Integer, Integer> expectedPairs = PairSet.of(1, 7);
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
		PairSet<Integer, Integer> expectedPairs = PairSet.of(1, 11).add(2, 13).add(3, 17).add(1, 23);
		assertCapturedPairsEqual(expectedPairs);
	}
	
	public void assertCapturedPairsEqual(PairSet<Integer, Integer> expectedPairs) {
		LinkedHashSet<Entry<Integer, Integer>> obtainedPairs = PairSet.toPairs(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		assertEquals(expectedPairs.asSet(), obtainedPairs);
	}
	
	@Test
	public void testSelect_hsqldb() throws SQLException {
		HSQLDBInMemoryDataSource dataSource = new HSQLDBInMemoryDataSource();
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		testInstance = persistenceContext.getPersister(Toto.class);
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		Toto t = testInstance.select(1);
		assertEquals(1, (Object) t.a);
		assertEquals(10, (Object) t.b);
		assertEquals(100, (Object) t.c);
		List<Toto> totos = testInstance.select(Arrays.asList((Serializable) 2, 3, 4));
		for (int i = 2; i <= 4; i++) {
			t = totos.get(i-2);
			assertEquals(i, (Object) t.a);
			assertEquals(10*i, (Object) t.b);
			assertEquals(100*i, (Object) t.c);
		}
	}
	
	private static class Toto {
		private Integer a, b, c;
		
		public Toto() {
		}
		
		public Toto(Integer a, Integer b, Integer c) {
			this.a = a;
			this.b = b;
			this.c = c;
		}
		
		public Toto(Integer b, Integer c) {
			this.b = b;
			this.c = c;
		}
		
		@Override
		public String toString() {
			return getClass().getSimpleName() + "["
					+ Maps.asMap("a", (Object) a).add("b", b).add("c", c)
					+ "]";
		}
	}
	
	/**
	 * Simple id gnerator for our tests : increments a in-memory counter..
	 */
	public static class InMemoryCounterIdentifierGenerator implements IdentifierGenerator {
		
		private AtomicInteger idCounter = new AtomicInteger(0);
		
		@Override
		public Serializable generate() {
			return idCounter.addAndGet(1);
		}
		
		@Override
		public void configure(Map<String, Object> configuration) {
			
		}
	}
	
}