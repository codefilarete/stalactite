package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.DELETE;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.INSERT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.UPDATE;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderCascadeTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private Persister<Person, PersistedIdentifier<Long>> personPersister;
	private Persister<City, PersistedIdentifier<Long>> cityPersister;
	private PersistenceContext persistenceContext;
	
	@BeforeClass
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Before
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderColumnOptions<Person, PersistedIdentifier<Long>> personMappingBuilder = FluentMappingBuilder.from(Person.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		personPersister = personMappingBuilder.build(persistenceContext);
		
		IFluentMappingBuilderColumnOptions<City, PersistedIdentifier<Long>> cityMappingBuilder = FluentMappingBuilder.from(City.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		cityPersister = cityMappingBuilder.build(persistenceContext);
	}
	
	@Test
	public void testCascade_oneToOne_noCascade() throws SQLException {
		expectedException.expectCause(
				both((Matcher<Throwable>) (Matcher) instanceOf(BatchUpdateException.class))
				.and(hasMessage(containsString("integrity constraint violation: foreign key no parent; FK_COUNTRY_PRESIDENTID_ID table: COUNTRY"))));
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::getPresident, personPersister)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country is persisted but not the president because we didn't asked for insert cascade
		// NB: we don't check with countryPersister.select(..) because select(..) depends on outer join which we don't want to be bothered by 
		
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 0");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person");
		assertFalse(resultSet.next());
	}
	
	@Test
	public void testCascade_oneToOne_insert() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(INSERT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country and the president are persisted all together since we asked for an insert cascade
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals("France president", persistedCountry.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// Creating a new country with the same president (!): the president shouldn't be resaved
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		countryPersister.insert(dummyCountry2);
		
		// Checking that the country is persisted but not the president since it has been previously
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("France president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		// President is cloned since we did nothing during select to reuse the existing one
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
	}
	
	@Test
	public void testCascade_oneToOne_insert_mandatory() throws SQLException {
		expectedException.expect(RuntimeMappingException.class);
		expectedException.expectMessage(new IsEqual<>("Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0"));
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(INSERT).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		countryPersister.insert(dummyCountry);
	}
	
	@Test
	public void testCascade_oneToOne_update() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(UPDATE)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		personPersister.insert(person);
		countryPersister.insert(dummyCountry);
		
		// Changing president's name to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.getPresident().setName("New France president");
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		// Checking that changing president's name is pushed to the database when we save the country
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		assertEquals("New France president", persistedCountry2.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
	}
	
	@Test
	public void testCascade_oneToOne_update_mandatory() throws SQLException {
		expectedException.expect(RuntimeMappingException.class);
		expectedException.expectMessage(new IsEqual<>("Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0"));
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(UPDATE).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		personPersister.insert(person);
		countryPersister.insert(dummyCountry);
		
		// Changing president's name to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.setPresident(null);
		countryPersister.update(persistedCountry, dummyCountry, true);
	}
	
	@Test
	public void testCascade_oneToOne_delete() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(DELETE)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
		
		Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
		countryPersister.delete(persistedCountry);
		ResultSet resultSet;
		// Checking that we deleted what we wanted
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 100");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 42");
		assertFalse(resultSet.next());
		// but we did'nt delete everything !
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 200");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person where id = 666");
		assertTrue(resultSet.next());
	}
	
	@Test
	public void testCascade_oneToOne_all() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(INSERT, UPDATE, DELETE)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals("France president", persistedCountry.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// testing insert cascade with another Country reusing OneToOne entity
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("France president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("France president renamed");
		countryPersister.update(persistedCountry2, persistedCountry, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertEquals("France president renamed", resultSet.getString("name"));
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertEquals(1, persistenceContext.getCurrentConnection().createStatement().executeUpdate(
				"update Country set presidentId = null where id = " + dummyCountry2.getId().getSurrogate()));
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 0");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person");
		assertFalse(resultSet.next());
	}
	
	@Test
	public void testCascade_multiple_oneToOne_all() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascade(INSERT, UPDATE, DELETE)
				.addOneToOne(Country::getCapital, cityPersister).cascade(INSERT, UPDATE, DELETE)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("France president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("France president", persistedCountry.getPresident().getName());
		assertEquals("Paris", persistedCountry.getCapital().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		assertTrue(persistedCountry.getCapital().getId().isPersisted());
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("France president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertEquals(persistedCountry.getCapital().getId().getSurrogate(), persistedCountry2.getCapital().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		assertNotSame(persistedCountry.getCapital(), persistedCountry2.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("France president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, persistedCountry, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertEquals("France president renamed", resultSet.getString("name"));
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertEquals("Paris renamed", resultSet.getString("name"));
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertEquals(1, persistenceContext.getCurrentConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getSurrogate()));
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 0");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Person");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City");
		assertFalse(resultSet.next());
	}
}
