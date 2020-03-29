package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.Strings;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableDelete;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableUpdate;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.DefaultResultSetReaders;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.result.ResultSetRowConverter;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.Test;

import static org.gama.lang.function.Functions.chain;
import static org.gama.stalactite.query.model.Operators.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextTest {
	
	@Test
	public void insert_Update_Delete() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new HSQLDBDialect());
		Table totoTable = new Table("toto");
		Column<Table, Long> id = totoTable.addColumn("id", long.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		// test insert
		testInstance.insert(totoTable).set(id, 1L).set(name, "Hello world !").execute();
		
		// test update
		int updatedRowCount = testInstance.update(totoTable).set(id, 1L).execute();
		assertEquals(1, updatedRowCount);
		
		ResultSet select_from_toto = connection.createStatement().executeQuery("select id, name from toto");
		select_from_toto.next();
		assertEquals(1, select_from_toto.getInt(1));
		assertEquals("Hello world !", select_from_toto.getString(2));
		
		// test update with where
		ExecutableUpdate set = testInstance.update(totoTable).set(id, 2L);
		set.where(id, eq(1L));
		updatedRowCount = set.execute();
		assertEquals(1, updatedRowCount);
		
		select_from_toto = connection.createStatement().executeQuery("select id from toto");
		select_from_toto.next();
		assertEquals(2, select_from_toto.getInt(1));
		
		// test delete
		ExecutableDelete delete = testInstance.delete(totoTable);
		delete.where(id, eq(2L));
		assertEquals(1, delete.execute());
	}
	
	
	@Test
	public void select() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new HSQLDBDialect());
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id);
		assertEquals(Arrays.asList(new Toto(1), new Toto(2)).toString(), records.toString());
		
		records = testInstance.select(Toto::new, id, name);
		assertEquals(Arrays.asList(new Toto(1, "Hello"), new Toto(2, "World")).toString(), records.toString());
		
		records = testInstance.select(Toto::new, id,
				select -> select.add(name, Toto::setName).add(name, Toto::setName2));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, Toto::getName2, ArrayList::new));
		
		records = testInstance.select(Toto::new, id,
				select -> select.add(name, Toto::setName),
				where -> where.and(id, eq(1)));
		assertEquals(Arrays.asList(new Toto(1, "Hello")).toString(), records.toString());
		
		records = testInstance.select(Toto::new, id, select -> 
				select.add(name, Toto::setName), where -> 
				where.and(id, eq(2)));
		assertEquals(Arrays.asList(new Toto(2, "World")).toString(), records.toString());
	}
	
	@Test
	public void select_columnTypeIsRegisteredInDialect() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = totoTable.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(Wrapper.class, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getJavaTypeToSqlTypeMapping().put(Wrapper.class, "varchar(255)");
				
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id, dummyProp);
		assertEquals(Arrays.asList(new Toto(1, new Wrapper("Hello")), new Toto(2, new Wrapper("World"))).toString(), records.toString());
		
		records = testInstance.select(Toto::new, id,
				select -> select.add(dummyProp, Toto::setDummyWrappedProp));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, chain(Toto::getDummyWrappedProp, Wrapper::getSurrogate), ArrayList::new));
	}
	
	@Test
	public void select_columnIsRegisteredInDialect_butNotItsType() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = totoTable.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(dummyProp, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getJavaTypeToSqlTypeMapping().put(dummyProp, "varchar(255)");
				
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id, dummyProp);
		assertEquals(Arrays.asList(new Toto(1, new Wrapper("Hello")), new Toto(2, new Wrapper("World"))).toString(), records.toString());
		
		records = testInstance.select(Toto::new, id,
				select -> select.add(dummyProp, Toto::setDummyWrappedProp));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, chain(Toto::getDummyWrappedProp, Wrapper::getSurrogate), ArrayList::new));
	}
	
	@Test
	public void newQuery() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.newQuery(QueryEase.select(id, name).from(totoTable), Toto.class)
				.mapKey(Toto::new, id, name)
				.execute();
		assertEquals(Arrays.asList(new Toto(1, "Hello"), new Toto(2, "World")).toString(), records.toString());
	}
	
	@Test
	public void newQuery_withRelation() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		Table tataTable = new Table("tata");
		Column<Table, String> tataName = tataTable.addColumn("name", String.class);
		Column<Table, Integer> totoId = tataTable.addColumn("totoId", int.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable, tataTable);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (1, 'World')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'Bonjour')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (2, 'Tout le monde')").execute();
		
		List<Toto> records = testInstance.newQuery(QueryEase.select(id, name).add(tataName, "tataName").from(totoTable).innerJoin(id, totoId), Toto.class)
				.mapKey(Toto::new, id, name)
				.map(Toto::setTata, new ResultSetRowConverter<>(Tata.class, "tataName", DefaultResultSetReaders.STRING_READER, Tata::new))
				.execute();
		Toto expectedToto1 = new Toto(1, "Hello");
		expectedToto1.setTata(new Tata("World"));
		Toto expectedToto2 = new Toto(2, "Bonjour");
		expectedToto2.setTata(new Tata("Tout le monde"));
		assertEquals(Arrays.asList(expectedToto1, expectedToto2).toString(), records.toString());
	}
	
	private static class Toto {
		
		private final int id;
		private String name;
		private String name2;
		private Wrapper dummyWrappedProp;
		private Tata tata;
		
		public Toto(int id) {
			this(id, (String) null);
		}
		
		public Toto(int id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public Toto(int id, Wrapper dummyProp) {
			this.id = id;
			this.dummyWrappedProp = dummyProp;
		}
		
		public int getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public String getName2() {
			return name2;
		}
		
		public void setName2(String name2) {
			this.name2 = name2;
		}
		
		public Tata getTata() {
			return tata;
		}
		
		public Toto setTata(Tata tata) {
			this.tata = tata;
			return this;
		}
		
		/** Implemented to ease debug and comparison for assertEquals(..) */
		@Override
		public String toString() {
			return Strings.footPrint(this, Toto::getId, Toto::getName, Toto::getDummyWrappedProp, Toto::getTata);
		}
		
		public Wrapper getDummyWrappedProp() {
			return dummyWrappedProp;
		}
		
		public void setDummyWrappedProp(Wrapper dummyWrappedProp) {
			this.dummyWrappedProp = dummyWrappedProp;
		}
	}
	
	private static class Wrapper {
		
		private final String surrogate;
		
		public Wrapper(String surrogate) {
			this.surrogate = surrogate;
		}
		
		public String getSurrogate() {
			return surrogate;
		}
		
		/** Implemented to ease debug and comparison for assertEquals(..) */
		@Override
		public String toString() {
			return Strings.footPrint(this, Wrapper::getSurrogate);
		}
	}
	
	private static class Tata {
		private String name;
		
		public Tata(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		/** Implemented to ease debug and comparison for assertEquals(..) */
		@Override
		public String toString() {
			return Strings.footPrint(this, Tata::getName);
		}
	}
}