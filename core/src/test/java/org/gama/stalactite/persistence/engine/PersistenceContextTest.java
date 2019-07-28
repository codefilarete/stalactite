package org.gama.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableDelete;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableUpdate;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.gama.lang.function.Functions.chain;
import static org.gama.stalactite.query.model.Operators.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class PersistenceContextTest {
	
	@Test
	public void testInsert_Update_Delete() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new HSQLDBDialect());
		Table toto = new Table("toto");
		Column<Table, Long> id = toto.addColumn("id", long.class);
		Column<Table, String> name = toto.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		// test insert
		testInstance.insert(toto).set(id, 1L).set(name, "Hello world !").execute();
		
		// test update
		int updatedRowCount = testInstance.update(toto).set(id, 1L).execute();
		assertEquals(1, updatedRowCount);
		
		ResultSet select_from_toto = connection.createStatement().executeQuery("select id, name from toto");
		select_from_toto.next();
		assertEquals(1, select_from_toto.getInt(1));
		assertEquals("Hello world !", select_from_toto.getString(2));
		
		// test update with where
		ExecutableUpdate set = testInstance.update(toto).set(id, 2L);
		set.where(id, eq(1L));
		updatedRowCount = set.execute();
		assertEquals(1, updatedRowCount);
		
		select_from_toto = connection.createStatement().executeQuery("select id from toto");
		select_from_toto.next();
		assertEquals(2, select_from_toto.getInt(1));
		
		// test delete
		ExecutableDelete delete = testInstance.delete(toto);
		delete.where(id, eq(2L));
		assertEquals(1, delete.execute());
	}
	
	
	@Test
	public void testSelect() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), new HSQLDBDialect());
		Table toto = new Table("toto");
		Column<Table, Integer> id = toto.addColumn("id", int.class);
		Column<Table, String> name = toto.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<TotoRecord> records = testInstance.select(TotoRecord::new, id);
		assertEquals(Arrays.asList(new TotoRecord(1), new TotoRecord(2)).toString(), records.toString());
		
		records = testInstance.select(TotoRecord::new, id, name);
		assertEquals(Arrays.asList(new TotoRecord(1, "Hello"), new TotoRecord(2, "World")).toString(), records.toString());
		
		records = testInstance.select(TotoRecord::new, id,
				select -> select.add(name, TotoRecord::setName).add(name, TotoRecord::setName2));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, TotoRecord::getName2, ArrayList::new));
		
		records = testInstance.select(TotoRecord::new, id,
				select -> select.add(name, TotoRecord::setName),
				where -> where.and(id, eq(1)));
		assertEquals(Arrays.asList(new TotoRecord(1, "Hello")).toString(), records.toString());
		
		records = testInstance.select(TotoRecord::new, id, select -> 
				select.add(name, TotoRecord::setName), where -> 
				where.and(id, eq(2)));
		assertEquals(Arrays.asList(new TotoRecord(2, "World")).toString(), records.toString());
	}
	
	@Test
	public void testSelect_columnTypeIsRegisteredInDialect() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table toto = new Table("toto");
		Column<Table, Integer> id = toto.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = toto.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(Wrapper.class, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getJavaTypeToSqlTypeMapping().put(Wrapper.class, "varchar(255)");
				
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<TotoRecord> records = testInstance.select(TotoRecord::new, id, dummyProp);
		assertEquals(Arrays.asList(new TotoRecord(1, new Wrapper("Hello")), new TotoRecord(2, new Wrapper("World"))).toString(), records.toString());
		
		records = testInstance.select(TotoRecord::new, id,
				select -> select.add(dummyProp, TotoRecord::setDummyWrappedProp));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, chain(TotoRecord::getDummyWrappedProp, Wrapper::getSurrogate), ArrayList::new));
	}
	
	@Test
	public void testSelect_columnIsRegisteredInDialect_butNotItsType() throws SQLException {
		HSQLDBInMemoryDataSource hsqldbInMemoryDataSource = new HSQLDBInMemoryDataSource();
		Connection connection = hsqldbInMemoryDataSource.getConnection();
		HSQLDBDialect dialect = new HSQLDBDialect();
		
		PersistenceContext testInstance = new PersistenceContext(new SimpleConnectionProvider(connection), dialect);
		Table toto = new Table("toto");
		Column<Table, Integer> id = toto.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = toto.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(dummyProp, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getJavaTypeToSqlTypeMapping().put(dummyProp, "varchar(255)");
				
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(toto);
		ddlDeployer.deployDDL();
		
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<TotoRecord> records = testInstance.select(TotoRecord::new, id, dummyProp);
		assertEquals(Arrays.asList(new TotoRecord(1, new Wrapper("Hello")), new TotoRecord(2, new Wrapper("World"))).toString(), records.toString());
		
		records = testInstance.select(TotoRecord::new, id,
				select -> select.add(dummyProp, TotoRecord::setDummyWrappedProp));
		assertEquals(Arrays.asList("Hello", "World"), Iterables.collect(records, chain(TotoRecord::getDummyWrappedProp, Wrapper::getSurrogate), ArrayList::new));
	}
	
	private static class TotoRecord {
		
		private final int id;
		private String name;
		private String name2;
		private Wrapper dummyWrappedProp;
		
		public TotoRecord(int id) {
			this(id, (String) null);
		}
		
		public TotoRecord(int id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public TotoRecord(int id, Wrapper dummyProp) {
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
		
		@Override
		public String toString() {
			return "TotoRecord{id=" + id + ", name='" + name + "\'}";
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
	}
}