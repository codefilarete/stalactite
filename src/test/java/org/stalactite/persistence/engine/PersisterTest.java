package org.stalactite.persistence.engine;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.mockito.ArgumentCaptor;
import org.stalactite.lang.collection.Arrays;
import org.stalactite.lang.collection.Maps;
import org.stalactite.persistence.mapping.ClassMappingStrategy;
import org.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class PersisterTest {
	
	private Persister<Toto> testInstance;
	private PersistenceContext persistenceContext;
	
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
		
		persistenceContext = new PersistenceContext(null);
		persistenceContext.add(totoClassMappingStrategy);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
	}
	
	@Test
	public void testPersist() throws Exception {
		PreparedStatement preparedStatement = mock(PreparedStatement.class);
		
		Connection connection = mock(Connection.class);
		ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
		when(connection.prepareStatement(argument.capture())).thenReturn(preparedStatement);
		
		ArgumentCaptor<Integer> valueCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> indexCaptor = ArgumentCaptor.forClass(Integer.class);
		
		DataSource dataSource = mock(DataSource.class);
		when(dataSource.getConnection()).thenReturn(connection);
		persistenceContext.setDataSource(dataSource);
		testInstance = new Persister<>(persistenceContext);
		testInstance.persist(new Toto(17, 23));
		
		verify(preparedStatement, times(1)).addBatch();
		verify(preparedStatement, times(3)).setInt(indexCaptor.capture(), valueCaptor.capture());
		assertEquals("insert into Toto(a, b, c) values (?, ?, ?)", argument.getValue());
		assertEquals(Arrays.asList(1, 2, 3), indexCaptor.getAllValues());
		assertEquals(Arrays.asList(1, 17, 23), valueCaptor.getAllValues());
	}
	
	@Test
	public void testDelete() throws Exception {
		
	}
	
	private static class Toto {
		private Integer a, b, c;
		
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