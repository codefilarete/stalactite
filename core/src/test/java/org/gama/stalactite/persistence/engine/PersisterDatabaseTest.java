package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.Retryer;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.test.DerbyInMemoryDataSource;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.sql.test.MariaDBEmbeddableDataSource;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class PersisterDatabaseTest {
	
	private Persister<Toto, Integer> testInstance;
	private JdbcConnectionProvider transactionManager;
	private InMemoryCounterIdentifierGenerator identifierGenerator;
	private ClassMappingStrategy<Toto, Integer> totoClassMappingStrategy;
	private Dialect dialect;
	private Table totoClassTable;
	
	@Before
	public void setUp() throws SQLException {
		totoClassTable = new Table(null, "Toto");
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor, Column> totoClassMapping = persistentFieldHarverster.mapFields(Toto.class, totoClassTable);
		Map<String, Column> columns = totoClassTable.mapColumnsOnName();
		columns.get("a").setPrimaryKey(true);
		
		identifierGenerator = new InMemoryCounterIdentifierGenerator();
		PropertyAccessor<Toto, Integer> identifierAccessor = PropertyAccessor.forProperty(persistentFieldHarverster.getField("a"));
		totoClassMappingStrategy = new ClassMappingStrategy<>(Toto.class, totoClassTable,
				totoClassMapping, identifierAccessor, new BeforeInsertIdentifierManager<>(IdMappingStrategy.toIdAccessor(identifierAccessor),
				identifierGenerator, Integer.class));
		
		JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
		simpleTypeMapping.put(Integer.class, "int");
		
		transactionManager = new JdbcConnectionProvider(null);
		dialect = new Dialect(simpleTypeMapping);
		
		// reset id counter between 2 tests else id "overflows"
		identifierGenerator.reset();
		
		testInstance = new Persister<>(totoClassMappingStrategy, transactionManager,
				new DMLGenerator(dialect.getColumnBinderRegistry() , DMLGenerator.CaseSensitiveSorter.INSTANCE), Retryer.NO_RETRY, 3, 3);
	}
	
	@DataProvider
	public static Object[][] dataSources() {
		return new Object[][] {
				{ new HSQLDBInMemoryDataSource() },
				{ new DerbyInMemoryDataSource() },
				{ new MariaDBEmbeddableDataSource(3406) },
		};
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testSelect(DataSource dataSource) throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlSchemaGenerator(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() throws SQLException {
				return dataSource.getConnection();
			}
		};
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(totoClassTable));
		ddlDeployer.deployDDL();
		Connection connection = dataSource.getConnection();
		connection.prepareStatement("insert into Toto(a, b, c) values (1, 10, 100)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (2, 20, 200)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (3, 30, 300)").execute();
		connection.prepareStatement("insert into Toto(a, b, c) values (4, 40, 400)").execute();
		connection.commit();
		List<Toto> totos = testInstance.select(Arrays.asList(1));
		Toto t = Iterables.first(totos);
		assertEquals(1, (Object) t.a);
		assertEquals(10, (Object) t.b);
		assertEquals(100, (Object) t.c);
		totos = testInstance.select(Arrays.asList(2, 3, 4));
		for (int i = 2; i <= 4; i++) {
			t = totos.get(i - 2);
			assertEquals(i, (Object) t.a);
			assertEquals(10 * i, (Object) t.b);
			assertEquals(100 * i, (Object) t.c);
		}
	}
	
	@Test
	@UseDataProvider("dataSources")
	public void testSelect_rowCount(DataSource dataSource) throws SQLException {
		transactionManager.setDataSource(dataSource);
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlSchemaGenerator(), transactionManager) {
			@Override
			protected Connection getCurrentConnection() throws SQLException {
				return dataSource.getConnection();
			}
		};
		ddlDeployer.getDdlSchemaGenerator().setTables(Arrays.asList(totoClassTable));
		ddlDeployer.deployDDL();
		
		
		// check inserted row count
		int insertedRowCount = testInstance.insert(new Toto(1, 10, 100));
		assertEquals(1, insertedRowCount);
		insertedRowCount = testInstance.insert(Arrays.asList(new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertEquals(3, insertedRowCount);
		
		// check updated row count roughly
		int updatedRoughlyRowCount = testInstance.updateRoughly(Arrays.asList(new Toto(1, 10, 100)));
		assertEquals(1, updatedRoughlyRowCount);
		updatedRoughlyRowCount = testInstance.insert(Arrays.asList(new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertEquals(3, updatedRoughlyRowCount);
		updatedRoughlyRowCount = testInstance.updateRoughly(Arrays.asList(new Toto(-1, 10, 100)));
		assertEquals(0, updatedRoughlyRowCount);
		
		// check updated row count
		int updatedFullyRowCount = testInstance.update(Arrays.asList(
				new AbstractMap.SimpleEntry<>(new Toto(1, 10, 100), new Toto(1, 10, 101))), true);
		assertEquals(1, updatedFullyRowCount);
		updatedFullyRowCount = testInstance.update(Arrays.asList(
				new AbstractMap.SimpleEntry<>(new Toto(1, 10, 101), new Toto(1, 10, 101))), true);
		assertEquals(0, updatedFullyRowCount);
		updatedFullyRowCount = testInstance.update(Arrays.asList(
				new AbstractMap.SimpleEntry<>(new Toto(2, 20, 200), new Toto(2, 20, 201)),
				new AbstractMap.SimpleEntry<>(new Toto(3, 30, 300), new Toto(3, 30, 301)),
				new AbstractMap.SimpleEntry<>(new Toto(4, 40, 400), new Toto(4, 40, 401))), true);
		assertEquals(3, updatedFullyRowCount);
		
		// check deleted row count
		int deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100)));
		assertEquals(1, deleteRowCount);
		deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100), new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertEquals(3, deleteRowCount);
		deleteRowCount = testInstance.delete(Arrays.asList(new Toto(1, 10, 100), new Toto(2, 20, 200), new Toto(3, 30, 300), new Toto(4, 40, 400)));
		assertEquals(0, deleteRowCount);
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