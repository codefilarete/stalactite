package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.function.ThrowingSupplier;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.State;
import org.gama.stalactite.persistence.engine.runtime.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.runtime.OptimizedUpdatePersister;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.gama.stalactite.sql.result.Row;
import org.gama.stalactite.sql.result.RowIterator;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static java.util.stream.Collectors.toSet;
import static org.gama.lang.test.Assertions.assertAllEquals;
import static org.gama.lang.test.Assertions.hasExceptionInCauses;
import static org.gama.lang.test.Assertions.hasMessage;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode.READ_ONLY;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportOneToManySetTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private static IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> CITY_MAPPING_CONFIGURATION;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
	}

	@BeforeEach
	void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CITY_MAPPING_CONFIGURATION = MappingEase.entityBuilder(City.class,
				Identifier.LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
	}
	
	@Test
	void mappedBy_foreignKeyIsCreated() throws SQLException {
		// mapping building thanks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
				Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(RelationMode.READ_ONLY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getConnectionProvider().getCurrentConnection();
		
		ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
				((IConfiguredPersister) countryPersister).getMappingStrategy().getTargetTable().getName().toUpperCase())) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		Set<String> foundForeignKey = Iterables.collect(() -> fkCityIterator, JdbcForeignKey::getSignature, HashSet::new);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_CITY_COUNTRYID_COUNTRY_ID", "CITY", "COUNTRYID", "COUNTRY", "ID");
		assertEquals(Arrays.asHashSet(expectedForeignKey.getSignature()), foundForeignKey);
	}
	
	@Test
	void mappedBy_set_crud() {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		
		IEntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManySet(Country::getCities, cityConfiguration)
					// we indicates that relation is owned by reverse side
					.mappedBy(City::getCountry).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		City grenoble = new City(new PersistableIdentifier<>(13L));
		grenoble.setName("Grenoble");
		country.addCity(grenoble);
		City lyon = new City(new PersistableIdentifier<>(17L));
		lyon.setName("Lyon");
		country.addCity(lyon);
		persister.insert(country);
		
		List<Long> cityCountryIds = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		
		assertEquals(Arrays.asSet(country.getId().getSurrogate()), new HashSet<>(cityCountryIds));
		
		// testing select
		Country loadedCountry = persister.select(country.getId());
		assertEquals(Arrays.asHashSet("Grenoble", "Lyon"), Iterables.collect(loadedCountry.getCities(), City::getName, HashSet::new));
		// ensuring that source is set on reverse side too
		assertEquals(loadedCountry, Iterables.first(loadedCountry.getCities()).getCountry());
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addCity(Iterables.first(country.getCities()));
		
		persister.update(modifiedCountry, country, false);
		
		cityCountryIds = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		assertEquals(Arrays.asSet(country.getId().getSurrogate(), null), new HashSet<>(cityCountryIds));
		
		// testing delete
		persister.delete(modifiedCountry);
		// referencing columns must be set to null (we didn't ask for delete orphan)
		cityCountryIds = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		assertEquals(Arrays.asSet((Long) null), new HashSet<>(cityCountryIds));
	}
	
	@Test
	void mappedBy_list_crud() {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		
		Table ancientCitiesTable = new Table("AncientCities");
		Column<?, Integer> idx = ancientCitiesTable.addColumn("idx", Integer.class);
		IEntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManyList(Country::getAncientCities, cityConfiguration)
					.mappedBy(City::getCountry).indexedBy(idx).cascading(ALL)
				.build(persistenceContext);

		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Country country = new Country(new PersistableIdentifier<>(1L));
		City grenoble = new City(new PersistableIdentifier<>(13L));
		grenoble.setName("Grenoble");
		country.addAncientCity(grenoble);
		City lyon = new City(new PersistableIdentifier<>(17L));
		lyon.setName("Lyon");
		country.addAncientCity(lyon);
		persister.insert(country);

		List<Long> cityCountryIds = persistenceContext.newQuery("select countryId from AncientCities", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		
		assertEquals(Arrays.asSet(country.getId().getSurrogate()), new HashSet<>(cityCountryIds));
		
		// testing select
		Country loadedCountry = persister.select(country.getId());
		assertEquals(Arrays.asHashSet("Grenoble", "Lyon"), Iterables.collect(loadedCountry.getAncientCities(), City::getName, HashSet::new));
		// ensuring that source is set on reverse side too
		assertEquals(loadedCountry, Iterables.first(loadedCountry.getAncientCities()).getCountry());
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addAncientCity(Iterables.first(country.getAncientCities()));
		
		persister.update(modifiedCountry, country, false);
		
		cityCountryIds = persistenceContext.newQuery("select countryId from AncientCities", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		assertEquals(Arrays.asSet(country.getId().getSurrogate(), null), new HashSet<>(cityCountryIds));
		
		// testing delete
		persister.delete(modifiedCountry);
		// referencing columns must be set to null (we didn't ask for delete orphan)
		cityCountryIds = persistenceContext.newQuery("select countryId from AncientCities", Long.class)
				.mapKey(i -> i, "countryId", Long.class)
				.execute();
		assertEquals(Arrays.asSet((Long) null), new HashSet<>(cityCountryIds));
	}
	
	@Test
	void associationTable_set_crud() {
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName);
		
		IEntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.addOneToManySet(Country::getCities, cityConfiguration)
					// we ask for reverse fix because our addCity method sets reverse side owner which can lead to problems while comparing instances
					.reverselySetBy(City::setCountry)
				// relation is not owned by reverse side
				//.mappedBy(City::getCountry).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		City grenoble = new City(new PersistableIdentifier<>(13L));
		grenoble.setName("Grenoble");
		country.addCity(grenoble);
		City lyon = new City(new PersistableIdentifier<>(17L));
		lyon.setName("Lyon");
		country.addCity(lyon);
		persister.insert(country);
		
		List<Duo> associatedIds = persistenceContext.newQuery("select Country_id, city_id from Country_cities", Duo.class)
				.mapKey(Duo::new, "Country_id", Long.class, "city_id", Long.class)
				.execute();
		
		assertEquals(Arrays.asSet(
				new Duo<>(country.getId().getSurrogate(), grenoble.getId().getSurrogate()),
				new Duo<>(country.getId().getSurrogate(), lyon.getId().getSurrogate()))
				, new HashSet<>(associatedIds));
		
		// testing select
		Country loadedCountry = persister.select(country.getId());
		assertEquals(Arrays.asHashSet("Grenoble", "Lyon"), Iterables.collect(loadedCountry.getCities(), City::getName, HashSet::new));
		// ensuring that source is set on reverse side too
		assertEquals(loadedCountry, Iterables.first(loadedCountry.getCities()).getCountry());
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addCity(Iterables.first(country.getCities()));
		
		persister.update(modifiedCountry, country, false);
		// there's only 1 relation in table
		List<Long> cityCountryIds = persistenceContext.newQuery("select Country_id from Country_cities", Long.class)
				.mapKey(i -> i, "Country_id", Long.class)
				.execute();
		assertEquals(Arrays.asSet(country.getId().getSurrogate()), new HashSet<>(cityCountryIds));
		
		// testing delete
		persister.delete(modifiedCountry);
		// Cities shouldn't be deleted (we didn't ask for delete orphan)
		List<Long> cityIds = persistenceContext.newQuery("select id from city", Long.class)
				.mapKey(i -> i, "id", Long.class)
				.execute();
		assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
	}
	
	@Test
	void mappedBy_noCascade_defaultCascadeIsAll() throws SQLException {
		// mapping building thanks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.add(Country::getDescription)
				// no cascade
				.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country dummyCountry = new Country(new PersistableIdentifier<>(42L));
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		City city = new City(new LongProvider().giveNewIdentifier());
		city.setName("Paris");
		dummyCountry.addCity(city);
		countryPersister.insert(dummyCountry);
		
		// Checking that country is persisted and its city because we didn't set relation mode and default is ALL
		// NB: we don't check with countryPersister.select(..) because select(..) depends on outer join which we don't want to be bothered by 
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
		assertTrue(resultSet.next());
		resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City");
		assertTrue(resultSet.next());
		
		// preparing next tests by relating a city to the existing country
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
		
		// select selects entity and relations
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertEquals("France", loadedCountry.getName());
		assertEquals("Paris", Iterables.first(loadedCountry.getCities()).getName());
		
		loadedCountry.setName("touched France");
		Iterables.first(loadedCountry.getCities()).setName("touched Paris");
		countryPersister.update(loadedCountry, dummyCountry, false);
		
		// city is left untouched because association is ALL (not ALL_ORPHAN_REMOVAL)
		assertEquals("Paris", persistenceContext.newQuery("select name from City where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute()
				.get(0));
		
		assertEquals("touched France", persistenceContext.newQuery("select name from Country where id = 42", String.class)
				.mapKey(String::new, "name", String.class)
				.execute()
				.get(0));
		
		// testing delete
		int rowCount = countryPersister.delete(loadedCountry);
		assertEquals(1, rowCount);
		
		assertEquals(Collections.emptyList(), persistenceContext.newQuery("select name from Country where id = 42", String.class)
				.mapKey(String::new, "name", String.class)
				.execute());
		
		// city is left untouched because association is ALL (not ALL_ORPHAN_REMOVAL)
		assertEquals("Paris", persistenceContext.newQuery("select name from City where id = 1", String.class)
				.mapKey(String::new, "name", String.class)
				.execute()
				.get(0));
	}
	
	@Test
	void mappedBy_entitiesAreLoaded() throws SQLException {
		// mapping building thanks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				// no cascade
				.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// preparing data
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into Country(id, name) values (42, 'France')").execute();
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into City(id, name, countryId) values (2, 'Lyon', 42)").execute();
		
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertEquals(Arrays.asHashSet("Paris", "Lyon"), Iterables.collect(loadedCountry.getCities(), City::getName, HashSet::new));
	}
	
	@Test
	void mappedBy_collectionFactory() throws SQLException {
		// mapping building thanks to fluent API
		IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Country::getName)
				.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION)
					.mappedBy(City::setCountry)
					// applying a Set that will sort cities by their name
					.initializeWith(() -> new TreeSet<>(Comparator.comparing(City::getName)))
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// preparing data
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into Country(id, name) values (42, 'France')").execute();
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
		persistenceContext.getConnectionProvider().getCurrentConnection().prepareStatement("insert into City(id, name, countryId) values (2, 'Lyon', 42)").execute();
		
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertEquals(TreeSet.class, loadedCountry.getCities().getClass());
		assertAllEquals(Arrays.asList("Lyon", "Paris"), loadedCountry.getCities(), Function.identity(), City::getName);
	}
	
	static Object[][] mappedBy_differentWays_data() {
		// we recreate all the context of our test, else we end up in a static/non-static variable and method conflict because @MethodSource
		// needs a static provider, whereas a majority of our variables are class attributes, and database schema must be erased between tests
		// to avoid duplicate table + FK name
		return new Object[][] {
				{ (ThrowingSupplier<IEntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by setter
							.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
				{ (ThrowingSupplier<IEntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by getter
							.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::getCountry)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
				{ (ThrowingSupplier<IEntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
					Table cityTable = new Table("city");
					Column<Table, Identifier> countryId = cityTable.addColumn("countryId", Identifier.class);
					IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.add(Country::getName)
							.add(Country::getDescription)
							// relation defined by column
							.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(countryId)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				}},
		};
	}
	
	@ParameterizedTest
	@MethodSource("mappedBy_differentWays_data")
	void mappedBy_differentWays(ThrowingSupplier<IEntityPersister<Country, Identifier<Long>>, SQLException> persisterSupplier) throws SQLException {
		
		IEntityPersister<Country, Identifier<Long>> countryPersister = persisterSupplier.get();
		
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
		// the reloaded country has no cities because those haven't been updated in database so the link is "broken" and still onto country 1
		assertEquals(null, persistedCountry2.getCities());
	}
	
	@Nested
	class CascadeReadOnly {
		
		@Test
		void testCascade_readOnly_reverseSideIsNotMapped() throws SQLException {
			// mapping building thanks to fluent API
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					// no cascade, nor reverse side
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id, name) values (1, 'France')");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id, name) values (10, 'French president')");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, city_Id) values (1, 10)");
			
			LongProvider countryIdProvider = new LongProvider(1);
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			LongProvider cityIdProvider = new LongProvider(10);
			City city = new City(cityIdProvider.giveNewIdentifier());
			city.setName("French president");
			
			Country selectedCountry = countryPersister.select(dummyCountry.getId());
			assertEquals(dummyCountry.getName(), selectedCountry.getName());
			assertEquals(city.getName(), Iterables.first(selectedCountry.getCities()).getName());
		}
	}
	
	@Nested
	class CascadeAll {
		
		@Test
		void update_mappedBy() {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL)
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
			// removing Paris
			// adding Grenoble
			// renaming Lyon
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.getCities().remove(paris);
			City grenoble = new City(cityIdProvider.giveNewIdentifier());
			grenoble.setName("Grenoble");
			persistedCountry.addCity(grenoble);
			Iterables.first(persistedCountry.getCities()).setName("changed");
			
			countryPersister.update(persistedCountry, dummyCountry, true);
			
			Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
			assertEquals(Arrays.asHashSet("changed", "Grenoble"), Iterables.collect(persistedCountry2.getCities(), City::getName, HashSet::new));
			// reverse link is up to date
			assertEquals(Arrays.asList(persistedCountry2, persistedCountry2), Iterables.collectToList(persistedCountry2.getCities(), City::getCountry));
		}
		
		@Test
		void update_noMappedBy_associationTableIsMaintained() {
			// mapping building thanks to fluent API
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					// no cascade, nor reverse side
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
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
			// removing Paris
			// adding Grenoble
			// renaming Lyon
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.getCities().remove(paris);
			City grenoble = new City(cityIdProvider.giveNewIdentifier());
			grenoble.setName("Grenoble");
			persistedCountry.addCity(grenoble);
			Iterables.first(persistedCountry.getCities()).setName("changed");
			
			countryPersister.update(persistedCountry, dummyCountry, true);
			
			Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
			assertEquals(Arrays.asHashSet("changed", "Grenoble"), Iterables.collect(persistedCountry2.getCities(), City::getName, HashSet::new));
			// reverse link is empty because mappedBy wasn't defined
			assertEquals(Arrays.asList(null, null), Iterables.collectToList(persistedCountry2.getCities(), City::getCountry));
		}
		
		@Test
		void update_associationTable() {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
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
			// removing Paris
			// adding Grenoble
			// renaming Lyon
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			persistedCountry.getCities().remove(paris);
			City grenoble = new City(cityIdProvider.giveNewIdentifier());
			grenoble.setName("Grenoble");
			persistedCountry.addCity(grenoble);
			Iterables.first(persistedCountry.getCities()).setName("changed");
			
			countryPersister.update(persistedCountry, dummyCountry, true);
			
			Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
			// Checking deletion : we did'nt asked for deletion of removed entities so all of them must be there
			assertEquals(Arrays.asHashSet(lyon, grenoble), persistedCountry2.getCities());
			// Checking update is done too
			assertEquals(Arrays.asHashSet("changed", "Grenoble"), Iterables.collect(persistedCountry2.getCities(), City::getName, HashSet::new));
		}
		
		@Test
		void update_multipleTimes() {
			IFluentMappingBuilderPropertyOptions<State, Identifier<Long>> stateMappingBuilder = MappingEase.entityBuilder(State.class,
					Identifier.LONG_TYPE)
					.add(State::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(State::getName);
			
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
					.addOneToManySet(Country::getStates, stateMappingBuilder).mappedBy(State::setCountry).cascading(ALL)
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
			State isere = new State(new PersistableIdentifier<>(stateIdProvider.giveNewIdentifier()));
			isere.setName("Isere");
			dummyCountry.addState(isere);
			State ain = new State(new PersistableIdentifier<>(stateIdProvider.giveNewIdentifier()));
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
			State ardeche = new State(new PersistableIdentifier<>(stateIdProvider.giveNewIdentifier()));
			ardeche.setName("ardeche");
			persistedCountry.addState(ardeche);
			Iterables.first(persistedCountry.getStates()).setName("changed");
			
			countryPersister.update(persistedCountry, dummyCountry, true);
			
			Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
			// Checking deletion : for cities we asked for deletion of removed entities so the reloaded instance must have the same content as the memory one
			// but we didn't for regions, so all of them must be there
			// (comparison are done on equals/hashCode => id)
			assertEquals(Arrays.asHashSet(lyon, grenoble), persistedCountry2.getCities());
			assertEquals(Arrays.asHashSet(ardeche, isere), persistedCountry2.getStates());
			// Checking update is done too
			assertEquals(Arrays.asHashSet("changed", "Grenoble"), persistedCountry2.getCities().stream().map(City::getName).collect(toSet()));
			assertEquals(Arrays.asHashSet("changed", "ardeche"), persistedCountry2.getStates().stream().map(State::getName).collect(toSet()));
			
			// Ain should'nt have been deleted because we didn't asked for orphan removal
			List<Long> loadedAin = persistenceContext.newQuery("select id from State where id = " + ain.getId().getSurrogate(), Long.class)
					.mapKey(Long::new, "id", long.class)
					.execute();
			assertNotNull(Iterables.first(loadedAin));
		}
		
		@Test
		void delete_mappedRelation() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id, countryId) values (100, 42), (200, 42), (300, 666)");
			
			Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			countryPersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertFalse(resultSet.next());
			// database owning side must be cut
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select countryId from City where id in (100, 200)");
			assertEquals(Arrays.asList(null, null), Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			}));
			// memory owning side must have been updated too (even if user hasn't explicitly cut the link) because cascade ALL doesn't remove orphans
			// but cut database link : memory should mirror this
			assertEquals(Arrays.asList(null, null), Iterables.collectToList(persistedCountry.getCities(), City::getCountry));
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
		}
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, city_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertFalse(resultSet.next());
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertEquals(Arrays.asList(100, 200), Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			}));
			
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertFalse(resultSet.next());
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertEquals(Arrays.asList(300), Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			}));
		}
		
	}
	
	
	
	@Nested
	class CascadeAllOrphanRemoval {
		
		@Test
		void update_mappedBy_removedElementsAreDeleted() {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
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
			// Checking deletion has been take into account : the reloaded instance contains cities that are the same as as the memory one
			// (comparison are done on equals/hashCode => id)
			assertEquals(Arrays.asHashSet(lyon, grenoble), persistedCountry2.getCities());
			// Checking update is done too
			assertEquals(Arrays.asHashSet("changed", "Grenoble"), persistedCountry2.getCities().stream().map(City::getName).collect(toSet()));
		}
		
		@Test
		void delete_mappedBy() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id, countryId) values (100, 42), (200, 42), (300, 666)");
			
			Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			countryPersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertFalse(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertFalse(resultSet.next());
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
		}
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, city_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertFalse(resultSet.next());
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertFalse(resultSet.next());
			
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertFalse(resultSet.next());
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertFalse(resultSet.next());
		}
	}
	
	@Nested
	class CascadeAssociationOnly {
		
		@Test
		void withoutAssociationTable_throwsException() {
			IFluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					// no cascade
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ASSOCIATION_ONLY);
			
			Assertions.assertThrows(() -> mappingBuilder.build(persistenceContext), hasExceptionInCauses(MappingConfigurationException.class)
					.andProjection(hasMessage(RelationMode.ASSOCIATION_ONLY + " is only relevent with an association table")));
		}
		
		@Test
		void insert_withAssociationTable_associationRecordsMustBeInserted_butNotTargetEntities() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// We need to insert target cities because they won't be inserted by ASSOCIATION_ONLY cascade
			// If they were inserted by cascade an constraint violation error will be thrown 
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			
			Country country1 = new Country(new PersistableIdentifier<>(42L));
			City city1 = new City(new PersistableIdentifier<>(100L));
			City city2 = new City(new PersistableIdentifier<>(200L));
			country1.addCity(city1);
			country1.addCity(city2);
			Country country2 = new Country(new PersistableIdentifier<>(666L));
			City city3 = new City(new PersistableIdentifier<>(300L));
			country2.addCity(city3);
			
			// testing insertion
			countryPersister.insert(Arrays.asList(country1, country2));
			
			ResultSet resultSet;
			// Checking that we inserted what we wanted
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertTrue(resultSet.next());
		}
		
		@Test
		void update_withAssociationTable_associationRecordsMustBeUpdated_butNotTargetEntities() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we need to insert target cities because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200)");
			
			Country country1 = new Country(new PersistableIdentifier<>(42L));
			City city1 = new City(new PersistableIdentifier<>(100L));
			City city2 = new City(new PersistableIdentifier<>(200L));
			country1.addCity(city1);
			country1.addCity(city2);
			
			countryPersister.insert(country1);
			
			// changinf values before update
			country1.setName("France");
			city1.setName("Grenoble");
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			
			ResultSet resultSet;
			// Checking that country name was updated
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from Country where id = 42");
			ResultSetIterator<Row> countryIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList("France"), Iterables.collectToList(() -> countryIterator, row -> row.get("name")));
			// .. but not its city name
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList((Object) null), Iterables.collectToList(() -> cityIterator, row -> row.get("name")));
			
			// removing city doesn't have any effect either
			assertEquals(2, country1.getCities().size());	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			country1.getCities().remove(city1);
			assertEquals(1, country1.getCities().size());	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator2 = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList((Object) null), Iterables.collectToList(() -> cityIterator2, row -> row.get("name")));
		}
		
		
		@Test
		void update_withAssociationTable_associationRecordsMustBeUpdated_butNotTargetEntities_list() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManyList(Country::getAncientCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we need to insert target cities because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200)");
			
			Country country1 = new Country(new PersistableIdentifier<>(42L));
			City city1 = new City(new PersistableIdentifier<>(100L));
			City city2 = new City(new PersistableIdentifier<>(200L));
			country1.addAncientCity(city1);
			country1.addAncientCity(city2);
			
			countryPersister.insert(country1);
			
			// changinf values before update
			country1.setName("France");
			city1.setName("Grenoble");
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			
			ResultSet resultSet;
			// Checking that country name was updated
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from Country where id = 42");
			ResultSetIterator<Row> countryIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList("France"), Iterables.collectToList(() -> countryIterator, row -> row.get("name")));
			// .. but not its city name
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList((Object) null), Iterables.collectToList(() -> cityIterator, row -> row.get("name")));
			
			// removing city doesn't have any effect either
			assertEquals(2, country1.getAncientCities().size());	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			country1.getAncientCities().remove(city1);
			assertEquals(1, country1.getAncientCities().size());	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator2 = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertEquals(Arrays.asList((Object) null), Iterables.collectToList(() -> cityIterator2, row -> row.get("name")));
		}
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted_butNotTargetEntities() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, city_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertFalse(resultSet.next());
			// target entities are deleted when an association table exists
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertTrue(resultSet.next());
			// but we did'nt delete everything !
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertTrue(resultSet.next());
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertFalse(resultSet.next());
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertFalse(resultSet.next());
			// target entities are deleted when an association table exists
			resultSet = persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeQuery("select id from City where id = 300");
			assertTrue(resultSet.next());
		}
	}
	
	@Nested
	class SelectWithEmptyRelationMustReturnEmptyCollection {
		
		@Test
		void noAssociationTable() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(READ_ONLY)
					.build(persistenceContext);
			
			// this is a configuration safeguard, thus we ensure that configuration matches test below
			assertNull(((OptimizedUpdatePersister<Country, Identifier<Long>>) countryPersister).getSurrogate()
					.getEntityJoinTree().getJoin("Country_Citys0"));
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(null, loadedCountry.getCities());
			
		}
		
		@Test
		void withAssociationTable() throws SQLException {
			IEntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.add(Country::getName)
					.add(Country::getDescription)
					.addOneToManySet(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getConnectionProvider().getCurrentConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertEquals(null, loadedCountry.getCities());
		}
	}
}
