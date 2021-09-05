package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.gama.lang.Strings;
import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.DefaultResultSetReaders;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.result.ResultSetRowTransformer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.lang.function.Functions.chain;

/**
 * @author Guillaume Mary
 */
public abstract class PersistenceContextITTest {
	
	protected abstract DataSource createDataSource();
	
	protected abstract Dialect createDialect();
	
	@Test
	void select() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(createDataSource(), createDialect());
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id);
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1), new Toto(2)).toString());
		
		records = testInstance.select(Toto::new, id, name);
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1, "Hello"), new Toto(2, "World")).toString());
		
		records = testInstance.select(Toto::new, id,
									  select -> select.add(name, Toto::setName).add(name, Toto::setName2));
		assertThat(records).extracting(Toto::getName2).isEqualTo(Arrays.asList("Hello", "World"));
		
		records = testInstance.select(Toto::new, id,
									  select -> select.add(name, Toto::setName),
									  where -> where.and(id, Operators.eq(1)));
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1, "Hello")).toString());
		
		records = testInstance.select(Toto::new, id,
									  select -> select.add(name, Toto::setName),
									  where -> where.and(id, Operators.eq(2)));
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(2, "World")).toString());
	}
	
	@Test
	void select_columnTypeIsRegisteredInDialect() throws SQLException {
		Dialect dialect = createDialect();
		PersistenceContext testInstance = new PersistenceContext(createDataSource(), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = totoTable.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(Wrapper.class, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getSqlTypeRegistry().put(Wrapper.class, "varchar(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id, dummyProp);
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1, new Wrapper("Hello")), new Toto(2, new Wrapper("World"))).toString());
		
		records = testInstance.select(Toto::new, id,
									  select -> select.add(dummyProp, Toto::setDummyWrappedProp));
		assertThat(records).extracting(chain(Toto::getDummyWrappedProp, Wrapper::getSurrogate)).isEqualTo(Arrays.asList("Hello", "World"));
	}
	
	@Test
	void select_columnIsRegisteredInDialect_butNotItsType() throws SQLException {
		Dialect dialect = createDialect();
		PersistenceContext testInstance = new PersistenceContext(createDataSource(), dialect);
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = totoTable.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(dummyProp, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getSqlTypeRegistry().put(dummyProp, "varchar(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, dummyProp) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, id, dummyProp);
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1, new Wrapper("Hello")), new Toto(2, new Wrapper("World"))).toString());
		
		records = testInstance.select(Toto::new, id,
									  select -> select.add(dummyProp, Toto::setDummyWrappedProp));
		assertThat(records).extracting(chain(Toto::getDummyWrappedProp, Wrapper::getSurrogate)).isEqualTo(Arrays.asList("Hello", "World"));
	}
	
	@Test
	void newQuery() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(createDataSource(), createDialect());
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.newQuery(QueryEase.select(id, name).from(totoTable), Toto.class)
				.mapKey(Toto::new, id, name)
				.execute();
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(1, "Hello"), new Toto(2, "World")).toString());
	}
	
	@Test
	void newQuery_withRelation() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(createDataSource(), createDialect());
		Table totoTable = new Table("toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		Table tataTable = new Table("tata");
		Column<Table, String> tataName = tataTable.addColumn("name", String.class);
		Column<Table, Integer> totoId = tataTable.addColumn("totoId", int.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable, tataTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (1, 'World')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'Bonjour')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (2, 'Tout le monde')").execute();
		
		List<Toto> records = testInstance.newQuery(QueryEase.select(id, name).add(tataName, "tataName").from(totoTable).innerJoin(id, totoId),
												   Toto.class)
				.mapKey(Toto::new, id, name)
				.map(Toto::setTata, new ResultSetRowTransformer<>(Tata.class, "tataName", DefaultResultSetReaders.STRING_READER, Tata::new))
				.execute();
		Toto expectedToto1 = new Toto(1, "Hello");
		expectedToto1.setTata(new Tata("World"));
		Toto expectedToto2 = new Toto(2, "Bonjour");
		expectedToto2.setTata(new Tata("Tout le monde"));
		assertThat(records.toString()).isEqualTo(Arrays.asList(expectedToto1, expectedToto2).toString());
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
		
		/** Implemented to ease debug and represention of failing test cases */
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
		
		/** Implemented to ease debug and represention of failing test cases */
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
		
		/** Implemented to ease debug and represention of failing test cases */
		@Override
		public String toString() {
			return Strings.footPrint(this, Tata::getName);
		}
	}
}