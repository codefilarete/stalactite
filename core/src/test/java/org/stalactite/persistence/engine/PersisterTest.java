package org.stalactite.persistence.engine;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.mockito.ArgumentCaptor;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.id.IdentifierGenerator;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.stalactite.test.HSQLDBInMemoryDataSource;
import org.stalactite.test.JdbcTransactionManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PersisterTest {
	
	private Persister<Toto> testInstance;
	private PersistenceContext persistenceContext;
	private PreparedStatement preparedStatement;
	private ArgumentCaptor<Integer> valueCaptor;
	private ArgumentCaptor<Integer> indexCaptor;
	private ArgumentCaptor<String> argumentCaptor;
	private JdbcTransactionManager transactionManager;
	private TotoClassIdGenerator identifierGenerator;
	
	@BeforeTest
	public void setUp() throws SQLException {
		Table totoClassTable = new Table(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new TotoClassIdGenerator();
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
		
		Connection connection = mock(Connection.class);
		argumentCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(argumentCaptor.capture())).thenReturn(preparedStatement);
		
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
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 17, 23), valueCaptor.getAllValues());
	}
	
	@Test
	public void testInsert_multiple() throws Exception {
		testInstance.insert(Arrays.asList(new Toto(17, 23), new Toto(29, 31), new Toto(37, 41), new Toto(43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 17, 23, 2, 29, 31, 3, 37, 41, 4, 43, 53), valueCaptor.getAllValues());
	}
	
	@Test
	public void testPersist_update() throws Exception {
		testInstance.persist(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(17, 23, 7), valueCaptor.getAllValues());
	}
	
	@Test
	public void testUpdate_multiple() throws Exception {
		testInstance.update(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(12)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(17, 23, 1, 29, 31, 2, 37, 41, 3, 43, 53, 4), valueCaptor.getAllValues());
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).executeBatch();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(7), valueCaptor.getAllValues());
	}
	
	@Test
	public void testDelete_multiple() throws Exception {
		testInstance.delete(Arrays.asList(new Toto(1, 17, 23), new Toto(2, 29, 31), new Toto(3, 37, 41), new Toto(4, 43, 53)));
		
		verify(preparedStatement, times(4)).addBatch();
		verify(preparedStatement, times(2)).executeBatch();
		verify(preparedStatement, times(4)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 1, 1, 1), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 2, 3, 4), valueCaptor.getAllValues());
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
		assertEquals("select a, b, c from Toto where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(7), valueCaptor.getAllValues());
	}
	
	@Test
	public void testSelect_hsqldb() throws SQLException {
		transactionManager.setDataSource(new HSQLDBInMemoryDataSource());
		persistenceContext.deployDDL();
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
	 * Simple générateur d'id pour nos tests. Se contente de renvoyer 1 systématiquement
	 */
	private static class TotoClassIdGenerator implements IdentifierGenerator {
		
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