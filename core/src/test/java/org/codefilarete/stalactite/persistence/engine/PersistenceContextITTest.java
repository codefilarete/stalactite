package org.codefilarete.stalactite.persistence.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer;
import org.codefilarete.stalactite.sql.test.DatabaseIntegrationTest;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.tool.function.Functions.chain;

/**
 * @author Guillaume Mary
 */
public abstract class PersistenceContextITTest extends DatabaseIntegrationTest {
	
	protected abstract Dialect createDialect();
	
	@Test
	void select() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, createDialect());
		Table totoTable = new Table("Toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = testInstance.getConnectionProvider().giveConnection();
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'World')").execute();
		
		List<Toto> records = testInstance.select(Toto::new, selectMapping -> selectMapping.add(name, Toto::setName));
		assertThat(records.toString()).isEqualTo(Arrays.asList(new Toto(-1, "Hello"), new Toto(-1, "World")).toString());
		
		records = testInstance.select(Toto::new, id);
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
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, dialect);
		Table totoTable = new Table("Toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, Wrapper> dummyProp = totoTable.addColumn("dummyProp", Wrapper.class);
		dialect.getColumnBinderRegistry().register(Wrapper.class, new NullAwareParameterBinder<>(
				new LambdaParameterBinder<>(DefaultParameterBinders.STRING_BINDER, Wrapper::new, Wrapper::getSurrogate)));
		dialect.getSqlTypeRegistry().put(Wrapper.class, "varchar(255)");
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable);
		ddlDeployer.deployDDL();
		
		Connection connection = connectionProvider.giveConnection();
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
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, dialect);
		Table totoTable = new Table("Toto");
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
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, createDialect());
		Table totoTable = new Table("Toto");
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
	void newQuery_withToOneRelation() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, createDialect());
		Table totoTable = new Table("Toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		Table tataTable = new Table("Tata");
		Column<Table, String> tataName = tataTable.addColumn("name", String.class);
		Column<Table, Integer> totoId = tataTable.addColumn("totoId", int.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable, tataTable);
		ddlDeployer.deployDDL();
		
		Connection connection = connectionProvider.giveConnection();
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
	
	@Test
	void newQuery_withToManyRelation() throws SQLException {
		PersistenceContext testInstance = new PersistenceContext(connectionProvider, createDialect());
		Table totoTable = new Table("Toto");
		Column<Table, Integer> id = totoTable.addColumn("id", int.class);
		Column<Table, String> name = totoTable.addColumn("name", String.class);
		Table tataTable = new Table("Tata");
		Column<Table, String> tataName = tataTable.addColumn("name", String.class);
		Column<Table, Integer> totoId = tataTable.addColumn("totoId", int.class);
		
		DDLDeployer ddlDeployer = new DDLDeployer(testInstance);
		ddlDeployer.getDdlGenerator().addTables(totoTable, tataTable);
		ddlDeployer.deployDDL();
		
		Connection connection = connectionProvider.giveConnection();
		connection.prepareStatement("insert into Toto(id, name) values (1, 'Hello')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (1, 'World')").execute();
		connection.prepareStatement("insert into Tata(totoId, name) values (1, 'Tout le monde')").execute();
		connection.prepareStatement("insert into Toto(id, name) values (2, 'Bonjour')").execute();
		
		BeanRelationFixer<Toto, Tata> tataCombiner = BeanRelationFixer.of(Toto::setTatas, Toto::getTatas, LinkedHashSet::new);
		
		List<Toto> records = testInstance.newQuery(QueryEase.select(id, name).add(tataName, "tataName").from(totoTable).leftOuterJoin(id, totoId),
												   Toto.class)
				.mapKey(Toto::new, id, name)
				.map(tataCombiner, new ResultSetRowTransformer<>(Tata.class, "tataName", DefaultResultSetReaders.STRING_READER, Tata::new))
				.execute();
		Toto expectedToto1 = new Toto(1, "Hello");
		expectedToto1.setTatas(Arrays.asSet(new Tata("World"), new Tata("Tout le monde")));
		Toto expectedToto2 = new Toto(2, "Bonjour");
		assertThat(records.toString()).isEqualTo(Arrays.asList(expectedToto1, expectedToto2).toString());
	}
	
	private static class Toto {
		
		private final int id;
		private String name;
		private String name2;
		private Wrapper dummyWrappedProp;
		private Tata tata;
		private Set<Tata> tatas;
		
		public Toto() {
			this(-1);
		}
		
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
		
		public Set<Tata> getTatas() {
			return tatas;
		}
		
		public Toto setTatas(Set<Tata> tatas) {
			this.tatas = tatas;
			return this;
		}
		
		/** Implemented to ease debug and represention of failing test cases */
		@Override
		public String toString() {
			return Strings.footPrint(this, Toto::getId, Toto::getName, Toto::getDummyWrappedProp, Toto::getTata, Toto::getTatas);
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