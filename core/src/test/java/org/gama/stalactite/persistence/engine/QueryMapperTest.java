package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.gama.lang.Strings;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultResultSetReaders;
import org.gama.stalactite.sql.result.InMemoryResultSet;
import org.gama.stalactite.sql.result.ResultSetRowTransformer;
import org.gama.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.lang.Nullable.nullable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class QueryMapperTest {
	
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
				{	// no given key
					new QueryMapper<>(TotoWithNoArgConstructor.class, dummySql, columnBinderRegistry)
						.map("id", Toto::setId, long.class)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive), expected },
				{	// constructor with 1 arg, column name, column type
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", long.class)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive), expected },
				{	// with Java Bean constructor (no args)
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
						.mapKey(Toto::new, "id", long.class)
						.map("id", Toto::setId)
						.map("name", Toto::setName, String.class)
						.map("active", Toto::setActive), expected },
				{	// constructor as factory method
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
							.mapKey(Toto::ofId, "id", long.class)
							.map("name", Toto::setName, String.class)
							.map("active", Toto::setActive), expected },
				{	// with Java Bean constructor (no args), Column API
					new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
							.mapKey(Toto::new, "id", long.class)
							.map("id", Toto::setId)
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
						.mapKey(Toto::new, "id", long.class, "name", String.class, "active", boolean.class), expected },
				{	// constructor as factory method without type
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
								.mapKey(Toto::ofId, "id")
								.map("name", Toto::setName, String.class)
								.map("active", Toto::setActive), expected },
				{	// no-arg constructor
						new QueryMapper<>(TotoWithNoArgConstructor.class, dummySql, columnBinderRegistry)
								.mapKey(TotoWithNoArgConstructor::new, "id", long.class)
								.map("id", Toto::setId, long.class)
								.map("name", Toto::setName, String.class)
								.map("active", Toto::setActive), expected },
				{	// constructor without giving type
						new QueryMapper<>(TotoWithOneArgConstructor.class, dummySql, columnBinderRegistry)
								.mapKey(TotoWithOneArgConstructor::new, "id")
								.map("name", Toto::setName, String.class)
								.map("active", Toto::setActive), expected },
				{	// constructor as factory method without type
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
								.mapKey(Toto::ofIdAndName, "id", "name")
								.map("active", Toto::setActive), expected },
				{	// constructor without giving type
						new QueryMapper<>(TotoWithTwoArgConstructor.class, dummySql, columnBinderRegistry)
								.mapKey(TotoWithTwoArgConstructor::new, "id", "name")
								.map("active", Toto::setActive), expected },
				{	// constructor as factory method without type
						new QueryMapper<>(Toto.class, dummySql, columnBinderRegistry)
								.mapKey(Toto::ofIdAndNameAndActive, "id", "name", "active")
								, expected },
				{	// constructor without giving type
						new QueryMapper<>(TotoWithThreeArgConstructor.class, dummySql, columnBinderRegistry)
								.mapKey(TotoWithThreeArgConstructor::new, "id", "name", "active")
								, expected },
		};
	}
	
	@ParameterizedTest
	@MethodSource("queryMapperAPI_basicUsage")
	void queryMapperAPI_basicUsage(QueryMapper<Toto> queryMapper, List<Toto> expected) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.forHashMap(String.class, Object.class).add("id", 42L).add("name", "coucou").add("active", true),
				Maps.forHashMap(String.class, Object.class).add("id", 43L).add("name", "hello").add("active", false)
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		assertThat(result.toString()).isEqualTo(expected.toString());
	}
	
	@Test
	void execute_noGivenKey_oneInstancePerRowIsCreated() {
		List<Map<String, Object>> resultSetData = Arrays.asList(
			Maps.forHashMap(String.class, Object.class).add("name", "coucou").add("active", true),
			Maps.forHashMap(String.class, Object.class).add("name", "hello").add("active", false),
			Maps.forHashMap(String.class, Object.class).add("name", "hola").add("active", false),
			Maps.forHashMap(String.class, Object.class).add("name", "salut").add("active", false)
		);
		
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		Table totoTable = new Table<>("Toto");
		totoTable.addColumn("name", String.class);
		totoTable.addColumn("active", boolean.class);
		
		QueryMapper testInstance = new QueryMapper<>(TotoWithNoArgConstructor.class, "never executed statement", columnBinderRegistry)
			.map("name", Toto::setName, String.class)
			.map("active", Toto::setActive);
		
		List<Toto> expected = Arrays.asList(
			new Toto(-1, "coucou", true),
			new Toto(-1, "hello", false),
			new Toto(-1, "hola", false),
			new Toto(-1, "salut", false)
		);
		
		List<Toto> result = invokeExecuteWithData(testInstance, resultSetData);
		
		assertThat(result.toString()).isEqualTo(expected.toString());
	}
	
	public static Object[][] queryMapperAPI_basicUsageWithConverter() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		Table toto = new Table<>("Toto");
		Column<Table, Long> id = toto.addColumn("id", Long.class).primaryKey();
		Column<Table, String> name = toto.addColumn("name", String.class);
		
		return new Object[][] {
				{ new QueryMapper<>(Toto.class, "select id, name from Toto", columnBinderRegistry)
							.mapKey(Toto::new, "id", long.class)
							.map("id", Toto::setId)
							.map("name", Toto::setName, input -> "coucou") },
				{ new QueryMapper<>(Toto.class, "select id, active from Toto", columnBinderRegistry)
							.mapKey(Toto::new, "id", long.class)
							.map("id", Toto::setId)
							.map("active", Toto::setName, boolean.class, input -> "coucou") },
				{ new QueryMapper<>(Toto.class, "select id, name from Toto", columnBinderRegistry)
							.mapKey(Toto::new, "id", long.class)
							.map("id", Toto::setId)
							.map(name, Toto::setName, input -> "coucou") }
		};
	}
	
	@ParameterizedTest
	@MethodSource("queryMapperAPI_basicUsageWithConverter")
	void newQuery_withConverter(QueryMapper<Toto> queryMapper) {
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih").add("active", false),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie").add("active", false)
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "coucou", false),
				new Toto(43, "coucou", false)
		);
		assertThat(result.toString()).isEqualTo(expected.toString());
	}
	
	/**
	 * Invokes {@link QueryMapper#execute(ConnectionProvider)} with given parameters, made only mutualize code
	 * 
	 * @param queryMapper query to execute
	 * @param resultSetData data to be returned by connection statement
	 * @return bean instances created by {@link QueryMapper#execute(ConnectionProvider)} 
	 */
	private <C> List<C> invokeExecuteWithData(QueryMapper<C> queryMapper, List<? extends Map<? extends String, ? extends Object>> resultSetData) {
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
	void execute_instanceHasParameter() throws SQLException {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		// NB: SQL String is there only for clarification but is never executed
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "select id, name from Toto where id in (:id)", columnBinderRegistry)
				.mapKey(Toto::new, "id", long.class)
				.set("id", Arrays.asList(1, 2), Integer.class);
		
		// Very simple data are necessary for the ResultSet since only id is mapped 
		List<Map<String, Object>> resultSetData = Arrays.asList(Maps.asHashMap("id", 42L));
		
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
		assertThat(captor.getAllValues()).isEqualTo(Arrays.asList(1, 2));
		// Checking instanciation was done
		assertThat(Iterables.first(result).toString()).isEqualTo(new Toto(42).toString());
	}
	
	@Test
	void execute_instanceHasAssembler() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		ModifiableInt assemblerCounter = new ModifiableInt();
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "Whatever SQL ... it not executed", columnBinderRegistry)
				.mapKey(Toto::new, "id", long.class)
				.add((rootBean, resultSet) -> rootBean.setName(resultSet.getString("name") + assemblerCounter.increment()));
		
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih"),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie"),
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih")
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "ghoeihvoih3", false),	// counter is expected to be run on each row : last overwrites previous
				new Toto(43, "oziuoie2", false),
				new Toto(42, "ghoeihvoih3", false)
		);
		assertThat(result.toString()).isEqualTo(expected.toString());
	}
	
	@Test
	void execute_instanceHasAssembler_thatRunOnce() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		ModifiableInt assemblerCounter = new ModifiableInt();
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "Whatever SQL ... it not executed", columnBinderRegistry)
				.mapKey(Toto::new, "id", long.class)
				.add((rootBean, resultSet) -> rootBean.setName(resultSet.getString("name") + assemblerCounter.increment()), AssemblyPolicy.ONCE_PER_BEAN);
		
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih"),
				Maps.asHashMap("id", (Object) 43L).add("name", "oziuoie"),
				Maps.asHashMap("id", (Object) 42L).add("name", "ghoeihvoih")
		);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "ghoeihvoih1", false),	// counter is expected to be run once per bean : first survives to next
				new Toto(43, "oziuoie2", false),
				new Toto(42, "ghoeihvoih1", false)
		);
		assertThat(result.toString()).isEqualTo(expected.toString());
	}
	
	@Test
	void execute_instanceHasRelation() {
		ColumnBinderRegistry columnBinderRegistry = new ColumnBinderRegistry();
		QueryMapper<Toto> queryMapper = new QueryMapper<>(Toto.class, "Whatever SQL ... it not executed", columnBinderRegistry)
				.mapKey(Toto::new, "id", long.class)
				.map("name", Toto::setName)
				.map(Toto::setTata, new ResultSetRowTransformer<>(Tata.class, "tataName", DefaultResultSetReaders.STRING_READER, Tata::new));
		
		List<Map<String, Object>> resultSetData = Arrays.asList(
				Maps.asHashMap("id", (Object) 42L).add("name", "John").add("tataName", "you"),
				Maps.asHashMap("id", (Object) 43L).add("name", "Bob").add("tataName", "me"),
				Maps.asHashMap("id", (Object) 42L).add("name", "John").add("tataName", "you")
				);
		
		List<Toto> result = invokeExecuteWithData(queryMapper, resultSetData);
		
		List<Toto> expected = Arrays.asList(
				new Toto(42, "John", false).setTata(new Tata("you")),
				new Toto(43, "Bob", false).setTata(new Tata("me")),
				new Toto(42, "John", false).setTata(new Tata("you"))
		);
		// Checking content by values
		assertThat(result.toString()).isEqualTo(expected.toString());
		
		// Checking content by object reference to ensure bean cache usage
		assertThat(result.get(2)).isSameAs(result.get(0));
		assertThat(result.get(2).getTata()).isSameAs(result.get(0).getTata());
		assertThat(result.get(2)).isNotSameAs(result.get(1));
		assertThat(result.get(2).getTata()).isNotSameAs(result.get(1).getTata());
	}
	
	public static class Toto {
		
		public static Toto ofId(long id) {
			return new Toto(id);
		}
		
		public static Toto ofIdAndName(long id, String name) {
			return new Toto(id, name);
		}
		
		public static Toto ofIdAndNameAndActive(long id, String name, boolean active) {
			return new Toto(id, name, active);
		}
		
		private long id;
		
		private String name;
		
		private boolean active;
		
		private Tata tata;
		
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
		
		public Toto setTata(Tata tata) {
			this.tata = tata;
			return this;
		}
		
		public Tata getTata() {
			return tata;
		}
		
		/**
		 * Implemented to ease debug and represention of failing test cases
		 * @return a chain containing attributes foot print
		 */
		@Override
		public String toString() {
			return Strings.footPrint(this, t -> t.id, t -> t.name, t -> t.active, t -> nullable(t.tata).map(Tata::getName).get());
		}
	}
	
	/** Class created to avoid casting of Toto::new as a no-arg Function in test */
	public static class TotoWithNoArgConstructor extends Toto {
		
		public TotoWithNoArgConstructor() {
			super(-1);
		}
	}
	
	/** Class created to avoid casting of Toto::new as a one-arg Function in test */
	public static class TotoWithOneArgConstructor extends Toto {
		
		public TotoWithOneArgConstructor(long id) {
			super(id);
		}
	}
	
	/** Class created to avoid casting of Toto::new as a two-args Function in test */
	public static class TotoWithTwoArgConstructor extends Toto {
		
		public TotoWithTwoArgConstructor(long id, String name) {
			super(id, name);
		}
		
	}
	
	/** Class created to avoid casting of Toto::new as a three-args Function in test */
	public static class TotoWithThreeArgConstructor extends Toto {
		
		public TotoWithThreeArgConstructor(long id, String name, boolean active) {
			super(id, name, active);
		}
		
	}
	
	private static class Tata {
		
		private final String name;
		
		public Tata(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
	}
}