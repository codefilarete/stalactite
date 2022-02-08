package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.DataSourceConnectionProvider;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
abstract class PersisterITTest {

	private DataSource dataSource;

	abstract DataSource createDataSource();
	
	abstract Dialect createDialect();
	
	private Persister<Toto, Integer, TotoTable> testInstance;
	private DataSourceConnectionProvider connectionProvider;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	
	@BeforeEach
	void setUp() throws SQLException {
		TotoTable totoClassTable = new TotoTable(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<TotoTable, Object>> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column<TotoTable, Object>> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		PropertyAccessor<Toto, Integer> identifierAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		ClassMappingStrategy<Toto, Integer, TotoTable> totoClassMappingStrategy = new ClassMappingStrategy<>(
				Toto.class,
				totoClassTable,
				totoClassMapping,
				identifierAccessor,
				new BeforeInsertIdentifierManager<>(new SinglePropertyIdAccessor<>(identifierAccessor), identifierGenerator, Integer.class)
		);
		
		this.dataSource = createDataSource();
		Dialect dialect = createDialect();
		
		connectionProvider = new DataSourceConnectionProvider(dataSource);
		
		// reset id counter between 2 tests else id "overflows"
		identifierGenerator.reset();
		
		testInstance = new Persister<>(totoClassMappingStrategy, dialect, new ConnectionConfigurationSupport(connectionProvider, 3));
		
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), connectionProvider);
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		connectionProvider.giveConnection().commit();
	}
	
	@Test
	void persist() throws SQLException {
		// we simulate a database state to test entity update
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		Toto toBeInserted = new Toto(null, 20, 200);
		Toto toBeUpdated = new Toto(persistedInstanceID, 11, 111);
		testInstance.persist(Arrays.asList(toBeInserted, toBeUpdated));
		connectionProvider.giveConnection().commit();
		
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
		connection.commit();
	}
	
	@Test
	void insert() throws SQLException {
		Toto toBeInserted = new Toto(1, 10, 100);
		testInstance.insert(toBeInserted);
		connectionProvider.giveConnection().commit();
		
		Connection connection = dataSource.getConnection();
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
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		testInstance.update(new Toto(persistedInstanceID, 11, 111), new Toto(persistedInstanceID, 10, 100), true);
		connectionProvider.giveConnection().commit();
		
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
		connection.commit();
		
		testInstance.updateById(new Toto(persistedInstanceID, 12, 122));
		connectionProvider.giveConnection().commit();
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(
				Maps.asMap("a", 1).add("b", 12).add("c", 122)));
	}
	
	@Test
	void delete() throws SQLException {
		// we simulate a database state to test entity update
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		testInstance.delete(new Toto(persistedInstanceID, 11, 111));
		connectionProvider.giveConnection().commit();
		
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
		connection.commit();
		
		// we simulate a database state to test entity update
		connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		testInstance.deleteById(new Toto(persistedInstanceID, 12, 122));
		connectionProvider.giveConnection().commit();
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList());
	}
	
	@Test
	void select() throws SQLException {
		Connection connection = dataSource.getConnection();
		connection.setAutoCommit(false);
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
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