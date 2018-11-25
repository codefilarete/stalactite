package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.ThrowingSupplier;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.State;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.stream.Collectors.toSet;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.DELETE;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.INSERT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.SELECT;
import static org.gama.stalactite.persistence.engine.CascadeOption.CascadeType.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderCollectionCascadeTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private static IFluentMappingBuilderColumnOptions<City, Identifier<Long>> CITY_MAPPING_BUILDER;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
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
	public void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CITY_MAPPING_BUILDER = FluentMappingBuilder.from(City.class,
				Identifier.LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);
		cityPersister = CITY_MAPPING_BUILDER.build(persistenceContext);
	}
	
	@Test
	public void testCascade_oneToMany_noCascade() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry)
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
	public void testCascade_oneToMany_cascade_select_reverseSideIsNotMapped() throws SQLException {
		// mapping building thantks to fluent API
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade, nor reverse side
				.addOneToManySet(Country::getCities, cityPersister).cascade(SELECT)
				.build(persistenceContext);
		
		// We declare the table that will store our relationship, and overall our List index
		// NB: names are hardcoded here because they are hardly accessible from outside of CascadeManyConfigurer
		Table countryCitiesTable = new Table("Country_citys");
		countryCitiesTable.addColumn("country_Id", Identifier.class).primaryKey();
		countryCitiesTable.addColumn("city_Id", Identifier.class).primaryKey();
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().addTables(countryCitiesTable);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id, name) values (1, 'France')");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into City(id, name) values (10, 'France president')");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country_citys(country_Id, city_Id) values (1, 10)");
		
		LongProvider countryIdProvider = new LongProvider(1);
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		LongProvider cityIdProvider = new LongProvider(10);
		City city = new City(cityIdProvider.giveNewIdentifier());
		city.setName("France president");
		
		Country selectedCountry = countryPersister.select(dummyCountry.getId());
		assertEquals(dummyCountry.getName(), selectedCountry.getName());
		assertEquals(city.getName(), Iterables.first(selectedCountry.getCities()).getName());
	}
	
	public static Object[][] oneToManyInsertData() {
		// we recreate all the context of our test, else we end up in a static/non-static variable and method conflict because @MethodSource
		// needs a static provider, whereas a majority of our variables are class attributes, and database schema must be erased between tests
		// to avoid duplicate table + FK name
		return new Object[][] {
				{ (ThrowingSupplier<Persister<Country, Identifier<Long>, ?>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					Persister<City, Identifier<Long>, ?> cityPersister = CITY_MAPPING_BUILDER.build(persistenceContext);
					Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by setter
							.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, SELECT)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
				{ (ThrowingSupplier<Persister<Country, Identifier<Long>, ?>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					Persister<City, Identifier<Long>, ?> cityPersister = CITY_MAPPING_BUILDER.build(persistenceContext);
					Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by getter
							.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::getCountry).cascade(INSERT, SELECT)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
				{ (ThrowingSupplier<Persister<Country, Identifier<Long>, ?>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					Persister<City, Identifier<Long>, ?> cityPersister = CITY_MAPPING_BUILDER.build(persistenceContext);
					Column<Table, Country> countryId = (Column<Table, Country>) (Column) cityPersister.getTargetTable().mapColumnsOnName().get("countryId");
					Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by column
							.addOneToManySet(Country::getCities, cityPersister).mappedBy(countryId).cascade(INSERT, SELECT)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
		};
	}
	
	@ParameterizedTest
	@MethodSource("oneToManyInsertData")
	public void testCascade_oneToMany_insert(ThrowingSupplier<Persister<Country, Identifier<Long>, ?>, SQLException> a) throws SQLException {
		
		Persister<Country, Identifier<Long>, ?> countryPersister = a.get();
		
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
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, UPDATE, SELECT)
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
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, UPDATE, SELECT).deleteRemoved()
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
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(DELETE, SELECT)
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
	
	@Nested
	public class SelectWithEmptyRelationMustReturnEmptyCollection {
		
		@Test
		public void test_noAssociationTable() throws SQLException {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(SELECT)
					.build(persistenceContext);
			
			// this is a configuration safeguard, thus we ensure that configuration matches test below
			assertNull(((JoinedTablesPersister<Country, Identifier<Long>, ?>) countryPersister).giveJoinedStrategy("Country_Citys0"));
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(Collections.emptySet(), loadedCountry.getCities());
			
		}
		
		@Test
		public void test_withAssociationTable() throws SQLException {
			Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, cityPersister).cascade(SELECT)
					.build(persistenceContext);
			
			// We declare the table that will store our relationship, and overall our List index
			// NB: names are hardcoded here because they are hardly accessible from outside of CascadeManyConfigurer
			Table countryCitiesTable = new Table("Country_citys");
			countryCitiesTable.addColumn("country_Id", Identifier.class).primaryKey();
			countryCitiesTable.addColumn("city_Id", Identifier.class).primaryKey();

			assertEquals(countryCitiesTable, ((JoinedTablesPersister<Country, Identifier<Long>, ?>) countryPersister).giveJoinedStrategy("Country_Citys0").getTargetTable());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.getDdlSchemaGenerator().addTables(countryCitiesTable);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(Collections.emptySet(), loadedCountry.getCities());
		}
	}
	
	@Test
	public void testCascade_oneToMany_delete_reverseSideIsNotMapped_associationRecordsMustBeDeleted() throws SQLException {
		Persister<Country, Identifier<Long>, ? extends Table> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, cityPersister).cascade(DELETE, SELECT)
				.build(persistenceContext);
		
		// We declare the table that will store our relationship, and overall our List index
		// NB: names are hardcoded here because they are hardly accessible from outside of CascadeManyConfigurer
		Table countryCitiesTable = new Table("Country_citys");
		Column country_id = countryCitiesTable.addColumn("country_Id", Identifier.class).primaryKey();
		Column city_id = countryCitiesTable.addColumn("city_Id", Identifier.class).primaryKey();
		Column countryPK = Iterables.first(((Table<?>) countryPersister.getTargetTable()).getPrimaryKey().getColumns());
		countryCitiesTable.addForeignKey("country_FK", country_id, countryPK);
		Column cityPK = Iterables.first(((Table<?>) cityPersister.getTargetTable()).getPrimaryKey().getColumns());
		countryCitiesTable.addForeignKey("city_FK", city_id, cityPK);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.getDdlSchemaGenerator().addTables(countryCitiesTable);
		ddlDeployer.deployDDL();
		
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
		persistenceContext.getCurrentConnection().createStatement().executeUpdate("insert into Country_citys(country_Id, city_Id)" +
				" values (42, 100), (42, 200), (666, 300)");
		
		Country country1 = new Country(new PersistedIdentifier<>(42L));
		City city1 = new City(new PersistedIdentifier<>(100L));
		City city2 = new City(new PersistedIdentifier<>(200L));
		country1.addCity(city1);
		country1.addCity(city2);
		Country country2 = new Country(new PersistedIdentifier<>(666L));
		City city3 = new City(new PersistedIdentifier<>(300L));
		country2.addCity(city3);

		// testing deletion
		countryPersister.delete(country1);

		ResultSet resultSet;
		// Checking that we deleted what we wanted
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
		assertFalse(resultSet.next());
		// NB: for now, target entities are not deleted when an association table exists
//		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
//		assertFalse(resultSet.next());
		// but we did'nt delete everything !
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
		assertTrue(resultSet.next());

		// testing deletion of the last one
		countryPersister.delete(country2);
		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
		assertFalse(resultSet.next());
		// NB: for now, target entities are not deleted when an association table exists
//		resultSet = persistenceContext.getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
//		assertFalse(resultSet.next());
	}
	
	@Test
	public void testCascade_multiple_oneToMany_update() throws SQLException {
		IFluentMappingBuilderColumnOptions<State, Identifier<Long>> stateMappingBuilder = FluentMappingBuilder.from(State.class,
				Identifier.LONG_TYPE)
				.add(State::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(State::getName)
				.add(State::getCountry);	// allow to declare the owner column of the relation
		Persister<State, Identifier<Long>, ?> statePersister = stateMappingBuilder.build(persistenceContext);
		
		
		Persister<Country, Identifier<Long>, ?> countryPersister = FluentMappingBuilder.from(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, cityPersister).mappedBy(City::setCountry).cascade(INSERT, UPDATE, SELECT).deleteRemoved()
				.addOneToManySet(Country::getStates, statePersister).mappedBy(State::setCountry).cascade(INSERT, UPDATE, SELECT)
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
		
		LongProvider stateIdProvider = new LongProvider();
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
