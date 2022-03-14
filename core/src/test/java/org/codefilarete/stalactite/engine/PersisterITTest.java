package org.codefilarete.stalactite.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.runtime.Persister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.id.manager.BeforeInsertIdentifierManager;
import org.codefilarete.stalactite.mapping.PersistentFieldHarverster;
import org.codefilarete.stalactite.mapping.SinglePropertyIdAccessor;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Database.Schema;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.test.DatabaseIntegrationTest;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
abstract class PersisterITTest extends DatabaseIntegrationTest {

	abstract Dialect createDialect();
	
	private Persister<Toto, Integer, TotoTable> testInstance;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	
	@BeforeEach
	void setUp() {
		TotoTable totoClassTable = new TotoTable(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<TotoTable, Object>> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column<TotoTable, Object>> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		// defining a test instance that maps Toto class onto TotoTable with "a" field as identifier
		PropertyAccessor<Toto, Integer> identifierAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		ClassMapping<Toto, Integer, TotoTable> totoClassMappingStrategy = new ClassMapping<>(
				Toto.class,
				totoClassTable,
				totoClassMapping,
				identifierAccessor,
				new BeforeInsertIdentifierManager<>(new SinglePropertyIdAccessor<>(identifierAccessor), identifierGenerator, Integer.class)
		);
		
		Dialect dialect = createDialect();
		
		// reset id counter between 2 tests else id "overflows"
		identifierGenerator.reset();
		
		testInstance = new Persister<>(totoClassMappingStrategy, dialect, new ConnectionConfigurationSupport(connectionProvider, 3));
		
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
	}
	
	@Test
	void persist() throws SQLException {
		// we simulate a database state to test entity update
		Connection connection = connectionProvider.giveConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values (" + persistedInstanceID + ", 10, 100)").execute();
		
		Toto toBeInserted = new Toto(null, 20, 200);
		Toto toBeUpdated = new Toto(persistedInstanceID, 11, 111);
		testInstance.persist(Arrays.asList(toBeInserted, toBeUpdated));
		
		ResultSetIterator<Map> resultSetIterator = new ResultSetIterator<Map>() {
			@Override
			public Map convert(ResultSet resultSet) throws SQLException {
				return Maps.asMap("a", resultSet.getObject("a")).add("b", resultSet.getObject("b")).add("c", resultSet.getObject("c"));
			}
		};
		ResultSet resultSet = connection.prepareStatement("select * from Toto order by a").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		List<Map> result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(
				Maps.asMap("a", 1).add("b", 11).add("c", 111),
				Maps.asMap("a", 2).add("b", 20).add("c", 200)));
	}
	
	@Test
	void insert() throws SQLException {
		Toto toBeInserted = new Toto(1, 10, 100);
		testInstance.insert(toBeInserted);
		
		Connection connection = connectionProvider.giveConnection();
		ResultSetIterator<Map> resultSetIterator = new ResultSetIterator<Map>() {
			@Override
			public Map convert(ResultSet resultSet) throws SQLException {
				return Maps.asMap("a", resultSet.getObject("a")).add("b", resultSet.getObject("b")).add("c", resultSet.getObject("c"));
			}
		};
		ResultSet resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		List<Map> result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(Maps.asMap("a", 1).add("b", 10).add("c", 100)));
	}
	
	@Test
	void update() throws SQLException {
		// we simulate a database state to test entity update
		Connection connection = connectionProvider.giveConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values (" + persistedInstanceID + ", 10, 100)").execute();
		
		testInstance.update(new Toto(persistedInstanceID, 11, 111), new Toto(persistedInstanceID, 10, 100), true);
		
		ResultSetIterator<Map> resultSetIterator = new ResultSetIterator<Map>() {
			@Override
			public Map convert(ResultSet resultSet) throws SQLException {
				return Maps.asMap("a", resultSet.getObject("a")).add("b", resultSet.getObject("b")).add("c", resultSet.getObject("c"));
			}
		};
		ResultSet resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		List<Map> result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(
				Maps.asMap("a", 1).add("b", 11).add("c", 111)));
		
		testInstance.updateById(new Toto(persistedInstanceID, 12, 122));
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(
				Maps.asMap("a", 1).add("b", 12).add("c", 122)));
	}
	
	@Test
	void delete() throws SQLException {
		// we simulate a database state to test entity update
		Connection connection = connectionProvider.giveConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values (" + persistedInstanceID + ", 10, 100)").execute();
		
		testInstance.delete(new Toto(persistedInstanceID, 11, 111));
		
		ResultSetIterator<Map> resultSetIterator = new ResultSetIterator<Map>() {
			@Override
			public Map convert(ResultSet resultSet) throws SQLException {
				return Maps.asMap("a", resultSet.getObject("a")).add("b", resultSet.getObject("b")).add("c", resultSet.getObject("c"));
			}
		};
		ResultSet resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		List<Map> result = Iterables.copy(resultSetIterator);
		// Result must be empty
		assertThat(result).isEqualTo(Arrays.asList());
		
		// we simulate a database state to test entity update
		connection.prepareStatement("insert into Toto(a, b, c) values (" + persistedInstanceID + ", 10, 100)").execute();
		
		testInstance.deleteById(new Toto(persistedInstanceID, 12, 122));
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList());
	}
	
	@Test
	void select() throws SQLException {
		Connection connection = connectionProvider.giveConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		
		List<Toto> totos = testInstance.select(Arrays.asList(1));
		Toto t = Iterables.first(totos);
		assertThat((Object) t.a).isEqualTo(1);
		assertThat((Object) t.b).isEqualTo(10);
		assertThat((Object) t.c).isEqualTo(100);
		totos = testInstance.select(Arrays.asList(2, 3, 4));
		
		List<Toto> expectedResult = Arrays.asList(
				new Toto(2, 20, 200),
				new Toto(3, 30, 300),
				new Toto(4, 40, 400));
		
		assertThat(totos.toString()).isEqualTo(expectedResult.toString());
	}
	
	static class Toto {
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
		
		/** Implemented to ease comparison on tests */
		@Override
		public String toString() {
			return "Toto{a=" + a + ", b=" + b + ", c=" + c + '}';
		}
	}
	
	static class TotoTable extends Table<TotoTable> {
		
		public TotoTable(String name) {
			super(name);
		}
		
		public TotoTable(Schema schema, String name) {
			super(schema, name);
		}
	}
}