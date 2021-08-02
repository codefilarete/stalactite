package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.Accessors;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Database.Schema;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
abstract class PersisterITTest {

	protected DataSource dataSource;

	@BeforeEach
	abstract void createDataSource();
	
	private Persister<Toto, Integer, TotoTable> testInstance;
	private JdbcConnectionProvider transactionManager;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	private ClassMappingStrategy<Toto, Integer, TotoTable> totoClassMappingStrategy;
	private Dialect dialect;
	private TotoTable totoClassTable;
	
	@BeforeEach
	void setUp() {
		totoClassTable = new TotoTable(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor<Toto, Object>, Column<TotoTable, Object>> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column<TotoTable, Object>> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		PropertyAccessor<Toto, Integer> identifierAccessor = Accessors.propertyAccessor(persistentFieldHarverster.getField("a"));
		totoClassMappingStrategy = new ClassMappingStrategy<>(
				Toto.class,
				totoClassTable,
				totoClassMapping,
				identifierAccessor,
				new BeforeInsertIdentifierManager<>(new SinglePropertyIdAccessor<>(identifierAccessor), identifierGenerator, Integer.class)
		);
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
		
		transactionManager = new JdbcConnectionProvider(null);
		dialect = new Dialect(simpleTypeMapping);
		
		// reset id counter between 2 tests else id "overflows"
		identifierGenerator.reset();
		
		testInstance = new Persister<>(totoClassMappingStrategy, dialect, new ConnectionConfigurationSupport(transactionManager, 3));
	}
	
	@Test
	void persist() throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		
		// we simulate a database state to test entity update
		Connection connection = dataSource.getConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		Toto toBeInserted = new Toto(null, 20, 200);
		Toto toBeUpdated = new Toto(persistedInstanceID, 11, 111);
		int rowCount = testInstance.persist(Arrays.asList(toBeInserted, toBeUpdated));
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(2);
		
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
				Maps.asMap("a", 1).add("b", 11).add("c", 111),
				Maps.asMap("a", 2).add("b", 20).add("c", 200)));
		connection.commit();
	}
	
	@Test
	void insert() throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		
		Toto toBeInserted = new Toto(1, 10, 100);
		int rowCount = testInstance.insert(toBeInserted);
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(1);
		
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
		connection.commit();
	}
	
	@Test
	void update() throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		
		// we simulate a database state to test entity update
		Connection connection = dataSource.getConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		int rowCount = testInstance.update(new Toto(persistedInstanceID, 11, 111), new Toto(persistedInstanceID, 10, 100), true);
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(1);
		
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
		
		rowCount = testInstance.updateById(new Toto(persistedInstanceID, 12, 122));
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(1);
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList(
				Maps.asMap("a", 1).add("b", 12).add("c", 122)));
		connection.commit();
	}
	
	@Test
	void delete() throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		
		// we simulate a database state to test entity update
		Connection connection = dataSource.getConnection();
		Integer persistedInstanceID = identifierGenerator.next();
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		int rowCount = testInstance.delete(new Toto(persistedInstanceID, 11, 111));
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(1);
		
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
		connection.prepareStatement("insert into Toto(a, b, c) values ("+ persistedInstanceID +", 10, 100)").execute();
		connection.commit();
		
		rowCount = testInstance.deleteById(new Toto(persistedInstanceID, 12, 122));
		transactionManager.getCurrentConnection().commit();
		assertThat(rowCount).isEqualTo(1);
		resultSet = connection.prepareStatement("select * from Toto").executeQuery();
		resultSetIterator.setResultSet(resultSet);
		result = Iterables.copy(resultSetIterator);
		assertThat(result).isEqualTo(Arrays.asList());
		connection.commit();
	}
	
	@Test
	void select() throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
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
	
	@Test
	void select_rowCount() {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getSqlTypeRegistry(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() {
				try {
					return dataSource.getConnection();
				} catch (SQLException e) {
					throw new SQLExecutionException(e);
				}
			}
		};
		ddlDeployer.getDdlGenerator().setTables(Arrays.asSet(totoClassTable));
		ddlDeployer.deployDDL();
		
		
		// check inserted row count
		int insertedRowCount = testInstance.insert(new Toto(1, 10, 100));
		assertThat(insertedRowCount).isEqualTo(1);
		insertedRowCount = testInstance.insert(Arrays.asList(new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertThat(insertedRowCount).isEqualTo(3);
		
		// check updated row count
		int updatedByIdRowCount = testInstance.updateById(Arrays.asList(new Toto(1, 10, 100)));
		assertThat(updatedByIdRowCount).isEqualTo(1);
		updatedByIdRowCount = testInstance.insert(Arrays.asList(new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertThat(updatedByIdRowCount).isEqualTo(3);
		updatedByIdRowCount = testInstance.updateById(Arrays.asList(new Toto(-1, 10, 100)));
		assertThat(updatedByIdRowCount).isEqualTo(0);
		
		// check updated row count
		int updatedFullyRowCount = testInstance.update(Arrays.asList(
				new Duo<>(new Toto(1, 10, 100), new Toto(1, 10, 101))), true);
		assertThat(updatedFullyRowCount).isEqualTo(1);
		updatedFullyRowCount = testInstance.update(Arrays.asList(
				new Duo<>(new Toto(1, 10, 101), new Toto(1, 10, 101))), true);
		assertThat(updatedFullyRowCount).isEqualTo(0);
		updatedFullyRowCount = testInstance.update(Arrays.asList(
				new Duo<>(new Toto(2, 20, 200), new Toto(2, 20, 201)),
				new Duo<>(new Toto(3, 30, 300), new Toto(3, 30, 301)),
				new Duo<>(new Toto(4, 40, 400), new Toto(4, 40, 401))), true);
		assertThat(updatedFullyRowCount).isEqualTo(3);
		
		// check deleted row count
		int deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100)));
		assertThat(deleteRowCount).isEqualTo(1);
		testInstance.getDeleteExecutor().setRowCountManager(RowCountManager.NOOP_ROW_COUNT_MANAGER);
		deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100), new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertThat(deleteRowCount).isEqualTo(3);
		deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100), new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertThat(deleteRowCount).isEqualTo(0);
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