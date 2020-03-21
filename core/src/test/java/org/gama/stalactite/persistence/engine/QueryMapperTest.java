package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.result.InMemoryResultSet;
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
public class QueryMapperTest {
	
	/**
	 * @return a {@link QueryMapper} as first argument and expected {@link Toto} built bean instance as second
	 */
	public static Object[][] queryMapperAPI_basicUsage() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		Table totoTable = new Table<>("Toto");
		Column<Table, Long> id = totoTable.addColumn("id", Long.class).primaryKey();
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		Column<Table, Boolean> active = totoTable.addColumn("active", boolean.class);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", true),
				new Toto(43, "hello", false)
		);
		
		String dummySql = "never executed statement";
		// we use different ways of mapping the same thing
		return new Object[][] {
				{	// default API: constructor with 1 arg, column name, column type
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", Long.class)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive), expected },
				{	// with Java Bean constructor (no args)
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive), expected },
				{	// with Java Bean constructor (no args)
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
							.mapKey(Toto::new, id, Toto::setId)
							.map(name, Toto::setName)
							.map(active, Toto::setActive), expected },
				{ 	// with Column API
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, id)
						.map(name, Toto::setName)
						.map(active, Toto::setActive), expected },
				{	// with Java Bean constructor with 2 arguments
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, id, name)
						.map(active, Toto::setActive), expected },
				{	// with Java Bean constructor with 3 arguments
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, id, name, active), expected },
				{	// with Java Bean constructor with 1 argument
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", long.class),
						Arrays.asList(
								new Toto(42, null, false),
								new Toto(43, null, false)
						) },
				{	// with Java Bean constructor with 2 arguments
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", long.class, "name", String.class),
						Arrays.asList(
								new Toto(42, "coucou", false),
								new Toto(43, "hello", false)
						) },
				{	// with Java Bean constructor with 3 arguments
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", long.class, "name", String.class, "active", boolean.class), expected}
		};
	}
	
	@ParameterizedTest
	@MethodSource("queryMapperAPI_basicUsage")
	public void queryMapperAPI_basicUsage(QueryMapper<Toto> queryMapper, List<Toto> expected) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "coucou").add("active", true),
				Maps.asHashMap("id", (Object) 43L).add("name", "hello").add("active", false)
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		assertEquals(expected.toString(), result.toString());
	}
	
	public static Object[][] queryMapperAPI_basicUsageWithConverter() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		Table toto = new Table<>("Toto");
		Column<Table, Long> id = toto.addColumn("id", Long.class).primaryKey();
		Column<Table, String> name = toto.addColumn("name", String.class);
		
		return new Object[][] {
				{ new QueryMapper<>(Toto.class, "select id, name from Toto", columnBinderRegistry)
							.mapKey(Toto::new, "id", Toto::setId)
							.map("name", Toto::setName, input -> "coucou") },
				{ new QueryMapper<>(Toto.class, "select id, active from Toto", columnBinderRegistry)
							.mapKey(Toto::new, "id", Toto::setId)
							.map("active", Toto::setName, boolean.class, input -> "coucou") },
				{ new QueryMapper<>(Toto.class, "select id, name from Toto", columnBinderRegistry)
							.mapKey(Toto::new, id, Toto::setId)
							.map(name, Toto::setName, input -> "coucou") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("queryMapperAPI_basicUsageWithConverter")
	public void testNewQuery_withConverter(QueryMapper<Toto> queryMapper) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih").add("active", false),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie").add("active", false)
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", false),
				new Toto(43, "coucou", false)
		);
		assertEquals(expected.toString(), result.toString());
	}
	
	/**
	 * Invokes {@link QueryMapper#execute(ConnectionProvider)} with given parameters, made only mutualize code
	 * 
	 * @param queryMapper query to execute
	 * @param resultSetData data to be returned by connection statement
	 * @return bean instances created by {@link QueryMapper#execute(ConnectionProvider)} 
	 */
	private List<Toto> invokeExecuteWithData(QueryMapper<Toto> queryMapper, List<Map<String, Object>> resultSetData) {
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
		
		return queryMapper.execute(() -> connectionMock);
	}
	
	@Test
	public void execute_instanceHasParameter() throws SQLException {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		// NB: SQL String is there only for clarification but is never executed
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "select id, name from Toto where id in (:id)", columnBinderRegistry)
				.mapKey(Toto::new, "id", Integer.class)
				.set("id", Arrays.asList(1, 2), Integer.class);
		
		// Very simple data are necessary for the ResultSet since only id is mapped 
		List<Map<String, Object>> resultSetData = Arrays.asList(Maps.asHashMap("id", 42));
		
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
		
		List<Toto> result = queryMapper.execute(() -> mock);
		// Checking that setters were called
		verify(statementMock, times(2)).setInt(anyInt(), captor.capture());
		assertEquals(Arrays.asList(1, 2), captor.getAllValues());
		// Checking instanciation was done
		assertEquals(new Toto(42).toString(), Iterables.first(result).toString());
	}
	
	@Test
	public void execute_instanceHasAssembler() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "select id, name from Toto", columnBinderRegistry)
				.mapKey(Toto::new, "id", Toto::setId)
				.add((rootBean, resultSet) -> rootBean.setName(resultSet.getString("name")));
		
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih"),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie")
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
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
		
		public Toto(long id, String name) {
			this.id = id;
			this.name = name;
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