package org.gama.stalactite.persistence.engine;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.collection.PairIterator;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcTransactionManager;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PersisterTest {
	
	public static final String TEST_UPDATE_DIFF_DATA = "testUpdate_diff";
	private Persister<Toto> testInstance;
	private PersistenceContext persistenceContext;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> statementArgCaptor;
	private JdbcTransactionManager transactionManager;
	private InMemoryIdGenerator identifierGenerator;
	
	@BeforeTest
	public void setUp() throws SQLException {
		Table totoClassTable = new Table(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryIdGenerator();
		ClassMappingStrategy<Toto> totoClassMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoClassTable, totoClassMapping, identifierGenerator);
		
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
		identifierGenerator.idCounter = 0;
		
		preparedStatement = mock(PreparedStatement.class);
		when(preparedStatement.executeBatch()).thenReturn(new int[] {1});
		
		Connection connection = mock(Connection.class);
		statementArgCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(statementArgCaptor.capture())).thenReturn(preparedStatement);
		
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		transactionManager.setDataSource(dataSource);
		testInstance = persistenceContext.getPersister(Toto.class);
		PersistenceContext.setCurrent(persistenceContext);
	}
	
	@Test
	public void testPersist_insert() throws Exception {
		testInstance.persist(new Toto(17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 2, 3),
				Arrays.asList(1, 17, 23));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testInsert_multiple() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3),
				Arrays.asList(1, 17, 23, 2, 29, 31, 3, 37, 41, 4, 43, 53));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testPersist_update() throws Exception {
		testInstance.persist(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 2, 3),
				Arrays.asList(17, 23, 7));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testUpdateRoughly_multiple() throws Exception {
		testInstance.updateRoughly(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3),
				Arrays.asList(17, 23, 1, 29, 31, 2, 37, 41, 3, 43, 53, 4));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@DataProvider(name = TEST_UPDATE_DIFF_DATA)
	public Object[][] testUpdate_diff_data() {
		return new Object[][] {
				// extreme case: no differences
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)), 
					Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2)),
					new ExpectedResult_TestUpdate_diff(0, 0, 0, Collections.EMPTY_LIST, Collections.EMPTY_LIST, Collections.EMPTY_LIST)},
				// case: always the same kind of modification: only "b" field
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3)), 
					Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3)),
					new ExpectedResult_TestUpdate_diff(3, 1, 6,
							Arrays.asList("update Toto set b = ? where a = ?"),
							Arrays.asList(1, 2, 1, 2, 1, 2),
							Arrays.asList(2, 1, 3, 2, 4, 3)) },
				// case: always the same kind of modification: only "b" field, but batch should be called twice
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
					Arrays.asList(new Toto(1, 2, 1), new Toto(2, 3, 2), new Toto(3, 4, 3), new Toto(4, 5, 4)),
					new ExpectedResult_TestUpdate_diff(4, 2, 8,
							Arrays.asList("update Toto set b = ? where a = ?"),
							Arrays.asList(1, 2, 1, 2, 1, 2, 1, 2),
							Arrays.asList(2, 1, 3, 2, 4, 3, 5, 4)) },
				// case: always the same kind of modification: "b" + "c" fields
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4)),
					Arrays.asList(new Toto(1, 11, 11), new Toto(2, 22, 22), new Toto(3, 33, 33), new Toto(4, 44, 44)),
					new ExpectedResult_TestUpdate_diff(4, 2, 12,
							Arrays.asList("update Toto set b = ?, c = ? where a = ?"),
							Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3),
							Arrays.asList(11, 11, 1, 22, 22, 2, 33, 33, 3, 44, 44, 4)) },
				// more complex case: mix of modification sort, with batch updates
				{ 	Arrays.asList(new Toto(1, 1, 1), new Toto(2, 2, 2), new Toto(3, 3, 3), new Toto(4, 4, 4), new Toto(5, 5, 5), new Toto(6, 6, 6), new Toto(7, 7, 7)),
					Arrays.asList(new Toto(1, 11, 1), new Toto(2, 22, 2), new Toto(3, 33, 3), new Toto(4, 44, 44), new Toto(5, 55, 55), new Toto(6, 66, 66), new Toto(7, 7, 7)),
					new ExpectedResult_TestUpdate_diff(6, 2, 15,
							Arrays.asList("update Toto set b = ? where a = ?", "update Toto set b = ?, c = ? where a = ?"),
							Arrays.asList(1, 2, 1, 2, 1, 2, 1, 2, 3, 1, 2, 3, 1, 2, 3),
							Arrays.asList(11, 1, 22, 2, 33, 3, 44, 44, 4, 55, 55, 5, 66, 66, 6)) },
		};
	}
	
	private static class ExpectedResult_TestUpdate_diff {
		private int addBatchCallCount;
		private int executeBatchCallCount;
		private int setIntCallCount;
		private List<String> updateStatements;
		private List<Integer> indexes;
		private List<Integer> values;
		
		public ExpectedResult_TestUpdate_diff(int addBatchCallCount, int executeBatchCallCount, int setIntCallCount, List<String> updateStatements, List<Integer> indexes, List<Integer> values) {
			this.addBatchCallCount = addBatchCallCount;
			this.executeBatchCallCount = executeBatchCallCount;
			this.setIntCallCount = setIntCallCount;
			this.updateStatements = updateStatements;
			this.indexes = indexes;
			this.values = values;
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
		assertEquals(expectedResult.indexes, indexCaptor.getAllValues());
		assertEquals(expectedResult.values, valueCaptor.getAllValues());
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
		assertEquals(Arrays.asList("update Toto set c = ?, b = ? where a = ?"), statementArgCaptor.getAllValues());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3),
				Arrays.asList(123, 17, 1, 31, 129, 2, 141, 137, 3, 153, 143, 4));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a = ?", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(Arrays.asList(1), Arrays.asList(7));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a = ?", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(
				Arrays.asList(1, 1, 1, 1),
				Arrays.asList(1, 2, 3, 4));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	
	@Test
	public void testSelect() throws Exception {
		// mocking executeQuery not to return null because select method will use the ResultSet
		ResultSet resultSetMock = mock(ResultSet.class);
		when(preparedStatement.executeQuery()).thenReturn(resultSetMock);
		ResultSetMetaData metaDataMock = mock(ResultSetMetaData.class);
		when(resultSetMock.getMetaData()).thenReturn(metaDataMock);
		when(metaDataMock.getColumnCount()).thenReturn(3);
		when(metaDataMock.getColumnName(1)).thenReturn("a");
		when(metaDataMock.getColumnName(2)).thenReturn("b");
		when(metaDataMock.getColumnName(3)).thenReturn("c");
		
		testInstance.select(7);
		
		verify(preparedStatement, times(1)).executeQuery();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("select a, b, c from Toto where a = ?", statementArgCaptor.getValue());
		PairIterator<Integer, Integer> expectedPairs = new PairIterator<>(Arrays.asList(1), Arrays.asList(7));
		assertCapturedPairsEqual(expectedPairs);
	}
	
	public void assertCapturedPairsEqual(PairIterator<Integer, Integer> expectedPairs) {
		PairIterator<Integer, Integer> obtainedPairs = new PairIterator<>(indexCaptor.getAllValues(), valueCaptor.getAllValues());
		assertEquals(new HashSet<>(Iterables.copy(expectedPairs)), new HashSet<>(Iterables.copy(obtainedPairs)));
	}
	
	@Test
	public void testSelect_hsqldb() throws SQLException {
		transactionManager.setDataSource(new HSQLDBInMemoryDataSource());
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		testInstance = persistenceContext.getPersister(Toto.class);
		Connection connection = persistenceContext.getCurrentConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 2, 3)").execute();
		connection.commit();
		Toto t = testInstance.select(1);
		assertEquals(1, (Object) t.a);
		assertEquals(2, (Object) t.b);
		assertEquals(3, (Object) t.c);
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
	private static class InMemoryIdGenerator implements IdentifierGenerator {
		
		private int idCounter = 0;
		
		@Override
		public Serializable generate() {
			return ++idCounter;
		}
		
		@Override
		public void configure(Map<String, Object> configuration) {
			
		}
	}
	
}