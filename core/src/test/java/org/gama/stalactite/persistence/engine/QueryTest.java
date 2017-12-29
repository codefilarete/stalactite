package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.binder.ParameterBinderProvider;
import org.gama.sql.result.InMemoryResultSet;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class QueryTest {
	
	@DataProvider
	public static Object[][] testNewQuery_data() {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		Table toto = new Table("Toto");
		Column<Long> id = toto.addColumn("id", Long.class).primaryKey();
		Column<String> name = toto.addColumn("name", String.class);
		Column<Boolean> active = toto.addColumn("active", boolean.class);
		
		return new Object[][] {
				{	// default API: column name, column type
					new Query<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Long.class, Toto::new)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{	// with Java Bean constructor (no args)
					new Query<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Long.class, Toto::new, Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{ 	// with Column API
					new Query<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey(id, Toto::new)
						.map(name, Toto::setName)
						.map(active, Toto::setActive) },
				{	// with Java Bean constructor (no args)
					new Query<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey(id, Toto::new, Toto::setId)
						.map(name, Toto::setName)
						.map(active, Toto::setActive) }
		};
	}
	
	@Test
	@UseDataProvider("testNewQuery_data")
	public void testNewQuery(Query<Toto> query) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "coucou").add("active", true),
				Maps.asHashMap("id", (Object) 43L).add("name", "hello").add("active", false)
		);
		
		// creation of a Connection that will give our test case data
		Connection connectionMock = mock(Connection.class);
		try {
			PreparedStatement statementMock = mock(PreparedStatement.class);
			when(connectionMock.prepareStatement(any())).thenReturn(statementMock);
			when(statementMock.executeQuery()).thenReturn(new InMemoryResultSet(resultSetData));
		} catch (SQLException e) {
			// impossible since there's no real database connection
			throw Exceptions.asRuntimeException(e);
		}
		
		List<Toto> result = query.execute(() -> connectionMock);
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", true), 
				new Toto(43, "hello", false)
		);
		assertEquals(expected.toString(), result.toString());
	}
	
	@Test
	public void testNewQuery_withCondition() throws SQLException {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		// NB: SQL String is there only for clarification but is never executed
		Query<Toto> query = new Query<>(Toto.class, "select id, name from Toto where id in (:id)", parameterBinderProvider)
				.mapKey("id", Integer.class, Toto::new)
				.set("id", Arrays.asList(1, 2), Integer.class);
		
		// Very simple data are necessary for the ResultSet since only id is mapped 
		Map<String, Object> testCaseData = Maps.asHashMap("id", 42);
		List<Map<String, Object>> resultSetData = Arrays.asList(testCaseData);
		
		// creation of a Connection that will give our test case data and will capture statement arguments
		Connection mock = mock(Connection.class);
		PreparedStatement statementMock;
		ArgumentCaptor<Integer> captor;
		try {
			statementMock = mock(PreparedStatement.class);
			captor = ArgumentCaptor.forClass(Integer.class);
			when(mock.prepareStatement(any())).thenReturn(statementMock);
			when(statementMock.executeQuery()).thenReturn(new InMemoryResultSet(resultSetData));
		} catch (SQLException e) {
			// impossible since there's no real database connection
			throw Exceptions.asRuntimeException(e);
		}
		
		List<Toto> result = query.execute(() -> mock);
		// Checking that setters were called
		verify(statementMock, times(2)).setInt(anyInt(), captor.capture());
		assertEquals(Arrays.asList(1, 2), captor.getAllValues());
		// Checking instanciation was done
		assertEquals(new Toto(42).toString(), Iterables.first(result).toString());
	}
	
//	@Test
//	public void testNewQuery_withColumn() {
//		PersistenceContext testInstance = new PersistenceContext(() -> null, new Dialect(new DefaultTypeMapping(), new ColumnBinderRegistry()));
//		Query query = testInstance.newQuery(new SelectQuery());
//		query.set("ids", Arrays.asList(1, 2));
//	}
	
//	@Test
//	public void testNewQuery_withBeanGraphBuilding() {
//		PersistenceContext testInstance = new PersistenceContext(() -> null, new Dialect(new DefaultTypeMapping(), new ColumnBinderRegistry()));
//		Query query = testInstance.newQuery(new SelectQuery());
//		query.set("ids", Arrays.asList(1, 2));
//	}
	
	public static class Toto {
		
		private long id;
		
		private String name;
		
		private boolean active;
		
		public Toto() {
		}
		
		public Toto(long id) {
			this.id = id;
		}
		
		public Toto(long id, String name, boolean active) {
			this.id = id;
			this.name = name;
			this.active = active;
		}
		
		public void setId(long id) {
			this.id = id;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public void setActive(boolean active) {
			this.active = active;
		}
		
		@Override
		public String toString() {
			return "Toto{id='" + id + '\'' + ", name='" + name + '\'' + ", c=" + active + '}';
		}
	}
}