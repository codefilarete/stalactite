package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.State;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.stream.Collectors.toSet;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.DELETE;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.INSERT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.SELECT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderCollectionCascadeTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
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
	
	@Before
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), DIALECT);
		
		IFluentMappingBuilderColumnOptions<City, PersistedIdentifier<Long>> personMappingBuilder = FluentMappingBuilder.from(City.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);	// allow to declare the owner column of the relation
		cityPersister = personMappingBuilder.build(persistenceContext);
	}
	
	@Test
	public void testCascade_oneToMany_noCascade() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToMany(Country::getCities, cityPersister).mappedBy(City::setCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		City city = new City(new LongProvider().giveNewIdentifier());
		city.setName("France president");
		dummyCountry.addCity(city);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country is persisted but not the president because we didn't asked for insert cascade
		// NB: we don't check with countryPersister.select(..) because select(..) depends on outer join which we don't want to be bothered by 
		ResultSet resultSet;
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 0");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City");
		assertFalse(resultSet.next());
	}
	
	@Test
	public void testCascade_oneToMany_insert() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToMany(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, SELECT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		LongProvider cityIdProvider = new LongProvider();
		City paris = new City(cityIdProvider.giveNewIdentifier());
		paris.setName("Paris");
		dummyCountry.addCity(paris);
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addCity(lyon);
		countryPersister.insert(dummyCountry);
		
		// Checking that the country and the cities are persisted all together since we asked for an insert cascade
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("Smelly cheese !", persistedCountry.getDescription());
		assertEquals(2, persistedCountry.getCities().size());
		assertEquals(Arrays.asSet("Paris", "Lyon"), persistedCountry.getCities().stream().map(City::getName).collect(toSet()));
		
		// Creating a new country with the same cities (!): the cities shouldn't be resaved
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.addCity(paris);
		dummyCountry2.addCity(lyon);
		countryPersister.insert(dummyCountry2);

		// Checking that the country is persisted but not the cities since they have been previously
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertEquals(new PersistedIdentifier<>(1L), persistedCountry2.getId());
		// the reloaded country has no cities because those hasn't been updated in database so the link is "broken" and still onto country 1
		assertEquals(0, persistedCountry2.getCities().size());
	}
	
	@Test
	public void testCascade_oneToMany_update() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToMany(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, UPDATE, SELECT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		LongProvider cityIdProvider = new LongProvider();
		City paris = new City(cityIdProvider.giveNewIdentifier());
		paris.setName("Paris");
		dummyCountry.addCity(paris);
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addCity(lyon);
		countryPersister.insert(dummyCountry);
		
		// Changing country cities to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.getCities().remove(paris);
		City grenoble = new City(cityIdProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		Iterables.first(persistedCountry.getCities()).setName("changed");
		
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		// Checking deletion : we did'nt asked for deletion of removed entities so all of them must be there
		// (comparison are done on equals/hashCode => id)
		assertEquals(Arrays.asHashSet(paris, lyon, grenoble), persistedCountry2.getCities());
		// Checking update is done too
		assertEquals(Arrays.asHashSet("Paris", "changed", "Grenoble"), persistedCountry2.getCities().stream().map(City::getName).collect(toSet()));
	}
	
	@Test
	public void testCascade_oneToMany_update_deleteRemoved() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToMany(Country::getCities, cityPersister)
					.mappedBy(City::setCountry)
					.cascade(INSERT, UPDATE, SELECT)
					.deleteRemoved()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		LongProvider cityIdProvider = new LongProvider();
		City paris = new City(cityIdProvider.giveNewIdentifier());
		paris.setName("Paris");
		dummyCountry.addCity(paris);
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addCity(lyon);
		countryPersister.insert(dummyCountry);
		
		// Changing country cities to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.getCities().remove(paris);
		City grenoble = new City(cityIdProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		Iterables.first(persistedCountry.getCities()).setName("changed");
		
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		// Checking deletion has been take into account : the reloaded instance contains cities that are the same as of the memory one
		// (comparison are done on equals/hashCode => id)
		assertEquals(Arrays.asHashSet(lyon, grenoble), persistedCountry2.getCities());
		// Checking update is done too
		assertEquals(Arrays.asHashSet("changed", "Grenoble"), persistedCountry2.getCities().stream().map(City::getName).collect(toSet()));
	}
	
	@Test
	public void testCascade_oneToMany_delete() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToMany(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(DELETE, SELECT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into City(id, countryId) values (100, 42), (200, 42), (300, 666)");
		
		Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		countryPersister.delete(persistedCountry);
		ResultSet resultSet;
		// Checking that we deleted what we wanted
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
		assertFalse(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
		assertFalse(resultSet.next());
		// but we did'nt delete everything !
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
		assertTrue(resultSet.next());
	}
	
	@Test
	public void testCascade_multiple_oneToMany_update() throws SQLException {
		// mapping building thantks to fluent API
		IFluentMappingBuilderColumnOptions<State, PersistedIdentifier<Long>> stateMappingBuilder = FluentMappingBuilder.from(State.class,
				(Class<PersistedIdentifier<Long>>) (Class) PersistedIdentifier.class)
				.add(State::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(State::getName)
				.add(State::getCountry);	// allow to declare the owner column of the relation
		Persister<State, PersistedIdentifier<Long>> statePersister = stateMappingBuilder.build(persistenceContext);
		
		
		Persister<Country, Identifier<Long>> countryPersister = FluentMappingBuilder.from(Country.class, (Class<Identifier<Long>>) (Class) PersistedIdentifier.class)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToMany(Country::getCities, cityPersister)
					.mappedBy(City::setCountry)
					.cascade(INSERT, UPDATE, SELECT)
					.deleteRemoved()
				.addOneToMany(Country::getStates, statePersister)
					.mappedBy(State::setCountry)
					.cascade(INSERT, UPDATE, SELECT)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		LongProvider cityIdProvider = new LongProvider(10); // NB: we start at a different index than other to avoid join collision
		City paris = new City(cityIdProvider.giveNewIdentifier());
		paris.setName("Paris");
		dummyCountry.addCity(paris);
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addCity(lyon);
		
		LongProvider stateIdProvider = new LongProvider(100); // NB: we start at a different index than other to avoid join collision
		State isere = new State(stateIdProvider.giveNewIdentifier());
		isere.setName("Isere");
		dummyCountry.addState(isere);
		State ain = new State(stateIdProvider.giveNewIdentifier());
		ain.setName("ain");
		dummyCountry.addState(ain);
		
		countryPersister.insert(dummyCountry);
		
		// Changing country cities to see what happens when we save it to the database
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		persistedCountry.getCities().remove(paris);
		City grenoble = new City(cityIdProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		Iterables.first(persistedCountry.getCities()).setName("changed");
		
		persistedCountry.getStates().remove(ain);
		State ardeche = new State(cityIdProvider.giveNewIdentifier());
		ardeche.setName("ardeche");
		persistedCountry.addState(ardeche);
		Iterables.first(persistedCountry.getStates()).setName("changed");
		
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		// Checking deletion : for cities we asked for deletion of removed entities so the reloaded instance must have the same content of the memory one
		// but we didn't for regions, so all of them must be there
		// (comparison are done on equals/hashCode => id)
		assertEquals(Arrays.asHashSet(lyon, grenoble), persistedCountry2.getCities());
		assertEquals(Arrays.asHashSet(ain, ardeche, isere), persistedCountry2.getStates());
		// Checking update is done too
		assertEquals(Arrays.asHashSet("changed", "Grenoble"), persistedCountry2.getCities().stream().map(City::getName).collect(toSet()));
		assertEquals(Arrays.asHashSet("ain", "changed", "ardeche"), persistedCountry2.getStates().stream().map(State::getName).collect(toSet()));
	}
	
	
}
