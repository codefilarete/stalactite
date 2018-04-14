package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.binder.ParameterBinderProvider;
import org.gama.sql.result.InMemoryResultSet;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
public class QueryConverterTest {
	
	public static Object[][] testNewQuery() {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		Table toto = new Table("Toto");
		Column<Long> id = toto.addColumn("id", Long.class).primaryKey();
		Column<String> name = toto.addColumn("name", String.class);
		Column<Boolean> active = toto.addColumn("active", boolean.class);
		
		return new Object[][] {
				{	// default API: column name, column type
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Toto::new, Long.class)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{	// with Java Bean constructor (no args)
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Toto::new, Toto::setId, Long.class)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{	// with Java Bean constructor (no args) and no column type
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Toto::new, Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{	// with Java Bean constructor (no args) and no column type, other syntax (not officially supported)
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey("id", Toto::new, (SerializableBiConsumer<Toto, Long>) Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive) },
				{	// with map method using other syntax (not officially supported)
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
							.mapKey("id", Toto::new, Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", (SerializableBiConsumer<Toto, Boolean>) Toto::setActive) },
				{ 	// with Column API
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey(id, Toto::new)
						.map(name, Toto::setName)
						.map(active, Toto::setActive) },
				{	// with Java Bean constructor (no args)
					new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
						.mapKey(id, Toto::new, Toto::setId)
						.map(name, Toto::setName)
						.map(active, Toto::setActive) }
		};
	}
	
	@ParameterizedTest
	@MethodSource("testNewQuery")
	public void testNewQuery(QueryConverter<Toto> queryConverter) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "coucou").add("active", true),
				Maps.asHashMap("id", (Object) 43L).add("name", "hello").add("active", false)
		);
		
		List<Toto> result = execute(queryConverter, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", true), 
				new Toto(43, "hello", false)
		);
		assertEquals(expected.toString(), result.toString());
	}
	
	public static Object[][] testNewQuery_withConverter() {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		Table toto = new Table("Toto");
		Column<Long> id = toto.addColumn("id", Long.class).primaryKey();
		Column<String> name = toto.addColumn("name", String.class);
		
		return new Object[][] {
				{ new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
							.mapKey("id", Toto::new, Toto::setId)
							.map("name", Toto::setName, input -> "coucou") },
				{ new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
							.mapKey("id", Toto::new, Toto::setId)
							.map("name", Toto::setName, String.class, input -> "coucou") },
				{ new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
							.mapKey(id, Toto::new, Toto::setId)
							.map(name, Toto::setName, input -> "coucou") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("testNewQuery_withConverter")
	public void testNewQuery_withConverter(QueryConverter<Toto> queryConverter) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih"),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie")
		);
		
		List<Toto> result = execute(queryConverter, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", false),
				new Toto(43, "coucou", false)
		);
		assertEquals(expected.toString(), result.toString());
	}
	
	public List<Toto> execute(QueryConverter<Toto> queryConverter, List<Map<String, Object>> resultSetData) {
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
		
		return queryConverter.execute(() -> connectionMock);
	}
	
	@Test
	public void testNewQuery_withCondition() throws SQLException {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		// NB: SQL String is there only for clarification but is never executed
		QueryConverter<Toto> queryConverter = new QueryConverter<>(Toto.class, "select id, name from Toto where id in (:id)", parameterBinderProvider)
				.mapKey("id", Toto::new, Integer.class)
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
		
		List<Toto> result = queryConverter.execute(() -> mock);
		// Checking that setters were called
		verify(statementMock, times(2)).setInt(anyInt(), captor.capture());
		assertEquals(Arrays.asList(1, 2), captor.getAllValues());
		// Checking instanciation was done
		assertEquals(new Toto(42).toString(), Iterables.first(result).toString());
	}
	
	@Test
	public void testNewQuery_addAssembler() {
		ParameterBinderProvider<Class> parameterBinderProvider = ParameterBinderProvider.fromMap(new ColumnBinderRegistry().getParameterBinders());
		QueryConverter<Toto> queryConverter = new QueryConverter<>(Toto.class, "select id, name from Toto", parameterBinderProvider)
				.mapKey("id", Toto::new, Toto::setId)
				.add((rootBean, resultSet) -> rootBean.setName(resultSet.getString("name")));
		
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih"),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie")
		);
		
		List<Toto> result = execute(queryConverter, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "ghoeihvoih", false),
				new Toto(43, "oziuoie", false)
		);
		assertEquals(expected.toString(), result.toString());
	}
	
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