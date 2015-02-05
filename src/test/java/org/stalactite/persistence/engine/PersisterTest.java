package org.stalactite.persistence.engine;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.mockito.ArgumentCaptor;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.sql.Dialect;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
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
	
	@BeforeTest
	public void setUp() throws SQLException {
		Table totoClassTable = new Table(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<Field, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		ClassMappingStrategy<Toto> totoClassMappingStrategy = new ClassMappingStrategy<Toto>(Toto.class, totoClassTable, totoClassMapping) {
			@Override
			public void fixId(Toto toto) {
				toto.a = 1;
			}
		};
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
		
		persistenceContext = new PersistenceContext(null, new Dialect(simpleTypeMapping));
		persistenceContext.add(totoClassMappingStrategy);
	}
	
	@BeforeMethod
	public void setUpTest() throws SQLException {
		preparedStatement = mock(PreparedStatement.class);
		
		Connection connection = mock(Connection.class);
		argumentCaptor = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(argumentCaptor.capture())).thenReturn(preparedStatement);
		
		valueCaptor = ArgumentCaptor.forClass(Integer.class);
		indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		persistenceContext.setDataSource(dataSource);
		testInstance = new Persister<>(persistenceContext);
	}
	
	@Test
	public void testPersist_insert() throws Exception {
		testInstance.persist(new Toto(17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 17, 23), valueCaptor.getAllValues());
	}
	
	@Test
	public void testPersist_update() throws Exception {
		testInstance.persist(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("update Toto set b = ?, c = ? where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(17, 23, 7), valueCaptor.getAllValues());
	}
	
	@Test
	public void testDelete() throws Exception {
		testInstance.delete(new Toto(7, 17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("delete Toto where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(7), valueCaptor.getAllValues());
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
		
		testInstance.select(Toto.class, 7);
		
		verify(preparedStatement, times(1)).executeQuery();
		verify(preparedStatement, times(1)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("select a, b, c from Toto where a = ?", argumentCaptor.getValue());
		assertEquals(Arrays.asList(1), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(7), valueCaptor.getAllValues());
	}
	
	@Test
	public void testSelect_hsqldb() throws SQLException {
		
		final Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test", "sa", "");
		persistenceContext.setDataSource(new DataSource() {
			@Override
			public Connection getConnection() throws SQLException {
				return connection;
			}
			
			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return null;
			}
			
			@Override
			public PrintWriter getLogWriter() throws SQLException {
				return null;
			}
			
			@Override
			public void setLogWriter(PrintWriter out) throws SQLException {
				
			}
			
			@Override
			public void setLoginTimeout(int seconds) throws SQLException {
				
			}
			
			@Override
			public int getLoginTimeout() throws SQLException {
				return 0;
			}
			
			@Override
			public Logger getParentLogger() throws SQLFeatureNotSupportedException {
				return null;
			}
			
			@Override
			public <T> T unwrap(Class<T> iface) throws SQLException {
				return null;
			}
			
			@Override
			public boolean isWrapperFor(Class<?> iface) throws SQLException {
				return false;
			}
		});
		persistenceContext.deployDDL();
		testInstance = new Persister<>(persistenceContext);
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 2, 3)").execute();
		connection.commit();
		Toto t = testInstance.select(Toto.class, 1);
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
}