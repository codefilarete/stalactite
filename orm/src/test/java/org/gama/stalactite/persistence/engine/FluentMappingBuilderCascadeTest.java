package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.lang.collection.Maps;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.result.RowIterator;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderOneToOneOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.AssertionFailedError;

import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ALL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationshipMode.ASSOCIATION_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderCascadeTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private Persister<Person, Identifier<Long>, ?> personPersister;
	private Persister<City, Identifier<Long>, ?> cityPersister;
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
	}
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderColumnOptions<Person, Identifier<Long>> personMappingBuilder = FluentMappingBuilder.from(Person.class, Identifier.LONG_TYPE)
				.add(Person::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		personPersister = personMappingBuilder.build(persistenceContext);
		
		IFluentMappingBuilderColumnOptions<City, Identifier<Long>> cityMappingBuilder = FluentMappingBuilder.from(City.class, Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		cityPersister = cityMappingBuilder.build(persistenceContext);
	}
	
	@Test
	public void testCascade_oneToOne_cascade_associationOnly_throwsException() {
		IFluentMappingBuilderOneToOneOptions<Country, Identifier<Long>> mappingBuilder = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::getPresident, personPersister).cascading(ASSOCIATION_ONLY);
		
		assertThrowsInHierarchy(() -> mappingBuilder.build(persistenceContext), MappingConfigurationException.class,
				RelationshipMode.ASSOCIATION_ONLY + " is only relevent for one-to-many association");
	}
	
	@Test
	public void testCascade_oneToOne_noCascade_defaultIsReadOnly() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToOne(Country::getPresident, personPersister)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country dummyCountry = new Country(new PersistableIdentifier<>(42L));
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new PersistableIdentifier<>(1L));
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		// insert throws integrity constraint because it doesn't save target entity
		assertThrowsInHierarchy(() -> countryPersister.insert(dummyCountry), BatchUpdateException.class,
				"integrity constraint violation: foreign key no parent; FK_COUNTRY_PRESIDENTID_PERSON_ID table: COUNTRY");
		
		persistenceContext.getCurrentConnection().prepareStatement("insert into Person(id, name) values (1, 'French president')").execute();
		persistenceContext.getCurrentConnection().prepareStatement("insert into Country(id, name, presidentId) values (42, 'France', 1)").execute();
		
		// select selects entity and relationship
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertEquals("France", loadedCountry.getName());
		assertEquals("French president", loadedCountry.getPresident().getName());
		
		loadedCountry.setName("touched France");
		loadedCountry.getPresident().setName("touched french president");
		countryPersister.update(loadedCountry, dummyCountry, false);
		
		// president is left untouched because association is read only
		assertEquals("French president", persistenceContext.newQuery("select name from Person where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.get(0));
		
		// deletion has no action on target
		countryPersister.delete(loadedCountry);
		assertTrue(persistenceContext.newQuery("select name from Country", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.isEmpty());
		assertEquals("French president", persistenceContext.newQuery("select name from Person where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute(persistenceContext.getConnectionProvider())
				.get(0));
	}
	
	static void assertThrowsInHierarchy(Executable executable, Class<? extends Throwable> expectedException, String expectedMessage) {
		try {
			executable.execute();
		} catch (Throwable actualException) {
			Throwable exceptionInHierarchy = Exceptions.findExceptionInHierarchy(actualException, expectedException);
			if (exceptionInHierarchy == null) {
				throw new AssertionFailedError("Unexpected exception thrown", expectedException, actualException);
			} else {
				assertEquals(expectedMessage, exceptionInHierarchy.getMessage());
				return;
			}
		}
		throw new AssertionFailedError("Expected exception to be thrown, but nothing was thrown.");
	}
	
	/**
	 * Thanks to the registering of Identified instances into the ColumnBinderRegistry (cf test preparation) it's possible to have a light relation
	 * between 2 mappings: kind of OneToOne without any cascade, just column of the relation is inserted/updated.
	 * Not really an expected feature since it looks like a OneToOne with insert+update cascade (on insert, already persisted instance are not inserted again)
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testCascade_lightOneToOne_relationIsPersisted() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.add(Country::getPresident)	// this is not a true relation, it's only for presidentId insert/update
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider personIdProvider = new LongProvider(123);
		Person person = new Person(personIdProvider.giveNewIdentifier());
		person.setName("France president");
		personPersister.insert(person);
		
		LongProvider countryIdProvider = new LongProvider(456);
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country has the tight president in the database
		ResultSet resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person.getId().getSurrogate());
		RowIterator resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertEquals(1, resultSetIterator.next().get("countryCount"));
		
		
		Country selectedCountry = countryPersister.select(dummyCountry.getId());
		// update test
		Person person2 = new Person(personIdProvider.giveNewIdentifier());
		person2.setName("French president");
		personPersister.insert(person2);
		dummyCountry.setPresident(person2);
		countryPersister.update(dummyCountry, selectedCountry, false);
		
		// Checking that the country has changed from president in the database
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery(
				"select count(*) as countryCount from Country where presidentId = " + person2.getId().getSurrogate());
		resultSetIterator = new RowIterator(resultSet, Maps.asMap("countryCount", DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER));
		resultSetIterator.hasNext();
		assertEquals(1, resultSetIterator.next().get("countryCount"));
	}
	
	@Test
	public void testCascade_oneToOne_insert() {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country and the president are persisted all together since we asked for an insert cascade
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// Creating a new country with the same president (!): the president shouldn't be resaved
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		countryPersister.insert(dummyCountry2);
		
		// Checking that the country is persisted but not the president since it has been previously
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		// President is cloned since we did nothing during select to reuse the existing one
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
	}
	
	@Test
	public void testCascade_oneToOne_insert_mandatory() {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		assertThrows(RuntimeMappingException.class, () -> countryPersister.insert(dummyCountry),
				"Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0");
	}
	
	@Test
	public void testCascade_oneToOne_update() {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		personPersister.insert(person);
		countryPersister.insert(dummyCountry);
		
		// Changing president's name to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.getPresident().setName("French president");
		countryPersister.update(persistedCountry, dummyCountry, true);
		// Checking that changing president's name is pushed to the database when we save the country
		Country countryFromDB = countryPersister.select(dummyCountry.getId());
		assertEquals("French president", countryFromDB.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// Changing president
		Person newPresident = new Person(new LongProvider().giveNewIdentifier());
		newPresident.setName("new French president");
		persistedCountry.setPresident(newPresident);
		countryPersister.update(persistedCountry, countryFromDB, true);
		// Checking that president has changed
		countryFromDB = countryPersister.select(dummyCountry.getId());
		assertEquals("new French president", countryFromDB.getPresident().getName());
		assertEquals(newPresident.getId().getSurrogate(), countryFromDB.getPresident().getId().getSurrogate());
	}
	
	@Test
	public void testCascade_oneToOne_update_mandatory() {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL).mandatory()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		personPersister.insert(person);
		countryPersister.insert(dummyCountry);
		
		// Changing president's name to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.setPresident(null);
		assertThrows(RuntimeMappingException.class, () -> countryPersister.update(persistedCountry, dummyCountry, true),
				"Non null value expected for relation o.g.s.p.e.m.Person o.g.s.p.e.m.Country.getPresident() on object org.gama.stalactite.persistence.engine.model.Country@0");
	}
	
	@Test
	public void testCascade_oneToOne_delete() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL)
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
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		
		// testing insert cascade with another Country reusing OneToOne entity
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		countryPersister.update(persistedCountry2, persistedCountry, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertEquals("French president renamed", resultSet.getString("name"));
		
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
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getDescription)
				.addOneToOne(Country::getPresident, personPersister).cascading(ALL)
				.addOneToOne(Country::getCapital, cityPersister).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("French president", persistedCountry.getPresident().getName());
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
		assertEquals("French president", persistedCountry2.getPresident().getName());
		assertEquals(persistedCountry.getPresident().getId().getSurrogate(), persistedCountry2.getPresident().getId().getSurrogate());
		assertEquals(persistedCountry.getCapital().getId().getSurrogate(), persistedCountry2.getCapital().getId().getSurrogate());
		assertNotSame(persistedCountry.getPresident(), persistedCountry2.getPresident());
		assertNotSame(persistedCountry.getCapital(), persistedCountry2.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, persistedCountry, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertEquals("French president renamed", resultSet.getString("name"));
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
