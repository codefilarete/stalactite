package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.MappingEase;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Town;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.OptimizedUpdatePersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.stalactite.sql.result.RowIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.function.ThrowingSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.READ_ONLY;
import static org.codefilarete.stalactite.dsl.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportOneToManySetTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private static FluentEntityMappingBuilder<City, Identifier<Long>> CITY_MAPPING_CONFIGURATION;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
	}

	@BeforeEach
	void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relation.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CITY_MAPPING_CONFIGURATION = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
	}
	
	@Nested
	class SchemaCreation {
		
		@Test
		void mappedBy_foreignKeyIsCreated() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
							Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(RelationMode.READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) countryPersister).getMapping().getTargetTable().getName().toUpperCase())) {
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
			assertThat(foundForeignKey).isEqualTo(Arrays.asHashSet(expectedForeignKey.getSignature()));
		}
		
		@Test
		void withTargetTable_targetTableIsUsed() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
							Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(City::getName)
							.onTable(new Table<>("Town")))
					.mappedBy(City::setCountry)
					.cascading(RelationMode.READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("COUNTRY", "TOWN");
			
			ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) countryPersister).getMapping().getTargetTable().getName().toUpperCase())) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_TOWN_COUNTRYID_COUNTRY_ID", "TOWN", "COUNTRYID", "COUNTRY", "ID");
			assertThat(foundForeignKey).isEqualTo(Arrays.asHashSet(expectedForeignKey.getSignature()));
		}
		
		@Test
		void withTargetTableSetByTargetEntity_tableSetByTargetEntityIsUSed() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class,
							Identifier.LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
							.onTable(new Table<>("Town"))
							.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(City::getName))
					.mappedBy(City::setCountry)
					.cascading(RelationMode.READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<Table> tableIterator = new ResultSetIterator<Table>(currentConnection.getMetaData().getTables(null, currentConnection.getSchema(),
					null, null)) {
				@Override
				public Table convert(ResultSet rs) throws SQLException {
					return new Table(
							rs.getString("TABLE_NAME")
					);
				}
			};
			Set<String> foundTables = Iterables.collect(() -> tableIterator, Table::getName, HashSet::new);
			assertThat(foundTables).containsExactlyInAnyOrder("COUNTRY", "TOWN");
			
			ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					((ConfiguredPersister) countryPersister).getMapping().getTargetTable().getName().toUpperCase())) {
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
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_TOWN_COUNTRYID_COUNTRY_ID", "TOWN", "COUNTRYID", "COUNTRY", "ID");
			assertThat(foundForeignKey).isEqualTo(Arrays.asHashSet(expectedForeignKey.getSignature()));
		}
	}
	
	@Nested
	class MappedBy {
		
		@Test
		void crud() {
			FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
					.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			
			EntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapOneToMany(Country::getCities, cityConfiguration)
					// we indicate that relation is owned by reverse side
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

			ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select countryId from city", Long.class)
					.mapKey(i -> i, "countryId", Long.class);
			Set<Long> cityCountryIds = longExecutableQuery2.execute(Accumulators.toSet());

			assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet(country.getId().getDelegate()));

			// testing select
			Country loadedCountry = persister.select(country.getId());
			assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Grenoble", "Lyon");
			// ensuring that source is set on reverse side too
			assertThat(Iterables.first(loadedCountry.getCities()).getCountry()).isEqualTo(loadedCountry);

			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId());
			modifiedCountry.addCity(Iterables.first(country.getCities()));

			persister.update(modifiedCountry, country, false);

			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select countryId from city", Long.class)
					.mapKey(i -> i, "countryId", Long.class);
			cityCountryIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet(country.getId().getDelegate(), null));
			
			// testing delete
			persister.delete(modifiedCountry);
			// referencing columns must be set to null (we didn't ask for delete orphan)
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select countryId from city", Long.class)
					.mapKey(i -> i, "countryId", Long.class);
			cityCountryIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet((Long) null));
		}
		
		@Test
		void ordered_crud() {
			FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
					.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			
			EntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapOneToMany(Country::getCities, cityConfiguration)
						// we indicate that relation is owned by reverse side
						.mappedBy(City::getCountry)
						.indexedBy("myIdx")
						.initializeWith(LinkedHashSet::new)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Country country = new Country(new PersistableIdentifier<>(1L));
			City grenoble = new City(new PersistableIdentifier<>(13L));
			grenoble.setName("Grenoble");
			City lyon = new City(new PersistableIdentifier<>(17L));
			lyon.setName("Lyon");
			City paris = new City(new PersistableIdentifier<>(23L));
			paris.setName("Paris");
			country.addCity(paris);
			country.addCity(grenoble);
			country.addCity(lyon);
			
			persister.insert(country);
			
			ExecutableQuery<Duo<String, Integer>> duoExecutableQuery1 = persistenceContext.newQuery("select name, myIdx from city", (Class<Duo<String, Integer>>) (Class) Duo.class)
					.mapKey(FluentEntityMappingConfigurationSupportOneToManySetTest::pair, "name", "myIdx");
			Set<Duo<String, Integer>> cityCountryIds = duoExecutableQuery1.execute(Accumulators.toSet());
			
			assertThat(cityCountryIds).containsExactlyInAnyOrder(new Duo<>("Paris", 1), new Duo<>("Grenoble", 2), new Duo<>("Lyon", 3));
			
			// testing select
			Country loadedCountry = persister.select(country.getId());
			assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactly("Paris", "Grenoble", "Lyon");
			// ensuring that source is set on reverse side too
			assertThat(Iterables.first(loadedCountry.getCities()).getCountry()).isEqualTo(loadedCountry);
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = loadedCountry;
			modifiedCountry.getCities().removeIf(city -> city.getName().equals("Grenoble"));
			
			persister.update(modifiedCountry, country, false);
			
			ExecutableQuery<Duo<String, Integer>> duoExecutableQuery = persistenceContext.newQuery("select name, myIdx from city", (Class<Duo<String, Integer>>) (Class) Duo.class)
					.mapKey(FluentEntityMappingConfigurationSupportOneToManySetTest::pair, "name", "myIdx");
			cityCountryIds = duoExecutableQuery.execute(Accumulators.toSet());
			
			assertThat(cityCountryIds).containsExactlyInAnyOrder(new Duo<>("Paris", 1), new Duo<>("Lyon", 2), new Duo<>("Grenoble", null));
		}
		
		@Test
		void noCascade_defaultCascadeIsAll() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
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
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City");
			assertThat(resultSet.next()).isTrue();
			
			// preparing next tests by relating a city to the existing country
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
			
			// select entity and relations
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getName()).isEqualTo("France");
			assertThat(Iterables.first(loadedCountry.getCities()).getName()).isEqualTo("Paris");
			
			loadedCountry.setName("touched France");
			Iterables.first(loadedCountry.getCities()).setName("touched Paris");
			countryPersister.update(loadedCountry, dummyCountry, false);
			
			// city is left untouched because association is ALL (not ALL_ORPHAN_REMOVAL)
			assertThat(persistenceContext.newQuery("select name from City where id = 1", String.class)
					.mapKey("name", String.class)
					.execute(Accumulators.getFirst()))
					.isEqualTo("Paris");
			
			assertThat(persistenceContext.newQuery("select name from Country where id = 42", String.class)
					.mapKey("name", String.class)
					.execute(Accumulators.getFirst()))
					.isEqualTo("touched France");
			
			// testing delete
			countryPersister.delete(loadedCountry);
			
			ExecutableQuery<String> stringExecutableQuery = persistenceContext.newQuery("select name from Country where id = 42", String.class)
					.mapKey("name", String.class);
			assertThat(stringExecutableQuery.execute(Accumulators.toSet())).isEmpty();
			
			// city is left untouched because association is ALL (not ALL_ORPHAN_REMOVAL)
			assertThat(persistenceContext.newQuery("select name from City where id = 1", String.class)
					.mapKey("name", String.class)
					.execute(Accumulators.getFirst()))
					.isEqualTo("Paris");
		}
		
		@Test
		void reverseEntitiesAreLoaded() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					// no cascade
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// preparing data
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Country(id, name) values (42, 'France')").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (2, 'Lyon', 42)").execute();
			
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Paris", "Lyon");
		}
		
		@Test
		void initializeWith() throws SQLException {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION)
					.mappedBy(City::setCountry)
					// applying a Set that will sort cities by their name
					.initializeWith(() -> new TreeSet<>(Comparator.comparing(City::getName)))
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// preparing data
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Country(id, name) values (42, 'France')").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
			persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (2, 'Lyon', 42)").execute();
			
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getCities().getClass()).isEqualTo(TreeSet.class);
			assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactly("Lyon", "Paris");
		}
		
		@Test
		void relationIsDefinedByColumnNameOnTargetSideAndReverseAccessorIsUsed_columnOverrideIsUsed() throws SQLException {
			FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(City::getName);
			
			FluentEntityMappingBuilder<Town, Identifier<Long>> townMappingBuilder = MappingEase.entityBuilder(Town.class, Identifier.LONG_TYPE)
					.mapSuperClass(cityMappingBuilder)
					.map(Town::getDiscotecCount);
			
			ConfiguredPersister<Country, Identifier<Long>> countryPersister =
					(ConfiguredPersister<Country, Identifier<Long>>) MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							// setting a foreign key naming strategy to be tested
							.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
							.mapOneToMany(Country::getTowns, townMappingBuilder).mappedBy(Town::getCountry).mappedBy("state")
							.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					countryPersister.getMapping().getTargetTable().getName().toUpperCase())) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			JdbcForeignKey foundForeignKey = Iterables.first(fkPersonIterator);
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_TOWN_STATE_COUNTRY_ID", "TOWN", "STATE", "COUNTRY", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
	}
	
	
	@Test
	void fetchSeparately() throws SQLException {
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				// no cascade
				.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
					.fetchSeparately()
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// preparing data
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into Country(id, name) values (42, 'France')").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (1, 'Paris', 42)").execute();
		persistenceContext.getConnectionProvider().giveConnection().prepareStatement("insert into City(id, name, countryId) values (2, 'Lyon', 42)").execute();
		
		Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
		assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Paris", "Lyon");
	}
	
	@Test
	void withPolymorphicTarget_crud() {
		FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName)
				.mapPolymorphism(PolymorphismPolicy.<City>joinTable()
						.addSubClass(MappingEase.subentityBuilder(Town.class, LONG_TYPE)));
		
		EntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapOneToMany(Country::getTowns, cityConfiguration)
				// we indicate that relation is owned by reverse side
				.mappedBy(City::getCountry).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Country country = new Country(new PersistableIdentifier<>(1L));
		Town grenoble = new Town(new PersistableIdentifier<>(13L));
		grenoble.setName("Grenoble");
		country.addTown(grenoble);
		Town lyon = new Town(new PersistableIdentifier<>(17L));
		lyon.setName("Lyon");
		country.addTown(lyon);
		persister.insert(country);
		
		ExecutableQuery<Long> longExecutableQuery2 = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class);
		Set<Long> cityCountryIds = longExecutableQuery2.execute(Accumulators.toSet());
		
		assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet(country.getId().getDelegate()));
		
		// testing select
		Country loadedCountry = persister.select(country.getId());
		assertThat(loadedCountry.getTowns()).extracting(City::getName).containsExactlyInAnyOrder("Grenoble", "Lyon");
		// ensuring that source is set on reverse side too
		assertThat(Iterables.first(loadedCountry.getTowns()).getCountry()).isEqualTo(loadedCountry);
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addTown(Iterables.first(country.getTowns()));
		
		persister.update(modifiedCountry, country, false);
		
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class);
		cityCountryIds = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet(country.getId().getDelegate(), null));
		
		// testing delete
		persister.delete(modifiedCountry);
		// referencing columns must be set to null (we didn't ask for delete orphan)
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select countryId from city", Long.class)
				.mapKey(i -> i, "countryId", Long.class);
		cityCountryIds = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(new HashSet<>(cityCountryIds)).isEqualTo(Arrays.asSet((Long) null));
	}
	
	private static Duo<String, Integer> pair(String name, Integer idx) {
		return new Duo<>(name, idx);
	}
	
	
	
	@Test
	void associationTable_set_crud() {
		FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		
		EntityPersister<Country, Identifier<Long>> persister = entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.mapOneToMany(Country::getCities, cityConfiguration)
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
		
		ExecutableQuery<Duo> duoExecutableQuery = persistenceContext.newQuery("select Country_id, cities_id from Country_cities", Duo.class)
				.mapKey(Duo::new, "Country_id", Long.class, "cities_id", Long.class);
		Set<Duo> associatedIds = duoExecutableQuery.execute(Accumulators.toSet());
		
		assertThat(associatedIds).containsExactlyInAnyOrder(
				new Duo<>(country.getId().getDelegate(), grenoble.getId().getDelegate()),
				new Duo<>(country.getId().getDelegate(), lyon.getId().getDelegate()));
		
		// testing select
		Country loadedCountry = persister.select(country.getId());
		assertThat(loadedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Grenoble", "Lyon");
		// ensuring that source is set on reverse side too
		assertThat(Iterables.first(loadedCountry.getCities()).getCountry()).isEqualTo(loadedCountry);
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addCity(Iterables.first(country.getCities()));
		
		persister.update(modifiedCountry, country, false);
		// there's only 1 relation in table
		ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select Country_id from Country_cities", Long.class)
				.mapKey(i -> i, "Country_id", Long.class);
		Set<Long> cityCountryIds = longExecutableQuery1.execute(Accumulators.toSet());
		assertThat(cityCountryIds).containsExactlyInAnyOrder(country.getId().getDelegate());
		
		// testing delete
		persister.delete(modifiedCountry);
		// Cities shouldn't be deleted (we didn't ask for delete orphan)
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from city", Long.class)
				.mapKey(i -> i, "id", Long.class);
		Set<Long> cityIds = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(cityIds).containsExactlyInAnyOrder(grenoble.getId().getDelegate(), lyon.getId().getDelegate());
	}
	
	static Object[][] mappedBy_differentWays_data() {
		// we recreate all the context of our test, else we end up in a static/non-static variable and method conflict because @MethodSource
		// needs a static provider, whereas a majority of our variables are class attributes, and database schema must be erased between tests
		// to avoid duplicate table + FK name
		return new Object[][] {
				{ (ThrowingSupplier<EntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
					EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
							// relation defined by setter
							.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				},
				true },
				{ (ThrowingSupplier<EntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
					EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
							// relation defined by getter
							.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::getCountry)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				},
				true },
				{ (ThrowingSupplier<EntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
					Table<?> cityTable = new Table("city");
					Column<?, Identifier<Long>> countryId = cityTable.addColumn("country_id", Identifier.LONG_TYPE);
					EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
							// relation defined by column
							.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(countryId)
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				},
				false },
				{ (ThrowingSupplier<EntityPersister<Country, Identifier<Long>>, SQLException>) () -> {
					PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
					EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
							.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.map(Country::getName)
							.map(Country::getDescription)
							// relation defined by column
							.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy("country_id")
							.cascading(ALL)
							.build(persistenceContext);
					DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
					ddlDeployer.deployDDL();
					return countryPersister;
				},
				false }
		};
	}
	
	@ParameterizedTest
	@MethodSource("mappedBy_differentWays_data")
	void mappedBy_differentWays(ThrowingSupplier<EntityPersister<Country, Identifier<Long>>, SQLException> persisterSupplier, boolean mappedByFunction) throws SQLException {
		
		EntityPersister<Country, Identifier<Long>> countryPersister = persisterSupplier.get();
		
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
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getDescription()).isEqualTo("Smelly cheese !");
		assertThat(persistedCountry.getCities().size()).isEqualTo(2);
		assertThat(persistedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Paris", "Lyon");
		if (mappedByFunction) {
			assertThat(persistedCountry.getCities()).extracting(City::getCountry).containsExactlyInAnyOrder(persistedCountry, persistedCountry);
		}
		
		// Creating a new country with the same cities (!): the cities should be associated to new country
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.addCity(paris);
		dummyCountry2.addCity(lyon);
		countryPersister.insert(dummyCountry2);

		// Checking that the country is associated to cities
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Paris", "Lyon");
		if (mappedByFunction) {
			assertThat(persistedCountry2.getCities()).extracting(City::getCountry).containsExactlyInAnyOrder(persistedCountry2, persistedCountry2);
		}
	}
	
	@Nested
	class CascadeReadOnly {
		
		@Test
		void insert_onlySourceEntitiesArePersisted() {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// cascade READ_ONLY, with relation table (no mappedBy)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION)
					.cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider(1);
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			LongProvider cityIdProvider = new LongProvider(10);
			City city = new City(cityIdProvider.giveNewIdentifier());
			city.setName("French president");
			countryPersister.insert(dummyCountry);
			
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from country", Long.class)
					.mapKey("id", Long.class);
			Set<Long> countryIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(countryIds).containsExactlyInAnyOrder(dummyCountry.getId().getDelegate());
			
			Long relationCount = persistenceContext.newQuery("select count(*) as relationCount from country_cities", Long.class)
					.mapKey("relationCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(relationCount).isEqualTo(0);
			
			Long cityCount = persistenceContext.newQuery("select count(*) as cityCount from city", Long.class)
					.mapKey("cityCount", Long.class)
					.execute(Accumulators.getFirst());
			assertThat(cityCount).isEqualTo(0);
		}
	}
	
	@Nested
	class CascadeAll {
		
		@Test
		void update_mappedBy() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL)
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
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
			// reverse link is up to date
			assertThat(persistedCountry2.getCities()).extracting(City::getCountry).containsExactly(persistedCountry2, persistedCountry2);
		}
		
		@Test
		void update_noMappedBy_associationTableIsMaintained() {
			// mapping building thanks to fluent API
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade, nor reverse side
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
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
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
			// reverse link is empty because mappedBy wasn't defined
			assertThat(persistedCountry2.getCities()).extracting(City::getCountry).containsExactly(null, null);
		}
		
		@Test
		void update_associationTable() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
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
			// Checking deletion : we didn't ask for deletion of removed entities so all of them must be there
			assertThat(persistedCountry2.getCities()).containsExactlyInAnyOrder(lyon, grenoble);
			// Checking update is done too
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void update_multipleTimes() {
			FluentEntityMappingBuilder<State, Identifier<Long>> stateMappingBuilder = MappingEase.entityBuilder(State.class, Identifier.LONG_TYPE)
					.mapKey(State::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(State::getName);
			
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
					.mapOneToMany(Country::getStates, stateMappingBuilder).mappedBy(State::setCountry).cascading(ALL)
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
			assertThat(persistedCountry2.getCities()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
			assertThat(persistedCountry2.getStates()).isEqualTo(Arrays.asHashSet(ardeche, isere));
			// Checking update is done too
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
			assertThat(persistedCountry2.getStates()).extracting(State::getName).containsExactlyInAnyOrder("changed", "ardeche");
			
			// Ain shouldn't have been deleted because we didn't ask for orphan removal
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from State where id = " + ain.getId().getDelegate(), Long.class)
					.mapKey(Long::new, "id", long.class);
			Set<Long> loadedAin = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(Iterables.first(loadedAin)).isNotNull();
		}
		
		@Test
		void delete_mappedRelation() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id, countryId) values (100, 42), (200, 42), (300, 666)");
			
			Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			countryPersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery(
					"select id from Country where id = 42");
			assertThat(resultSet.next()).isFalse();
			// database owning side must be cut
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery(
					"select countryId from City where id in (100, 200)");
			assertThat(Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			})).isEqualTo(Arrays.asList(null, null));
			// memory owning side must have been updated too (even if user hasn't explicitly cut the link) because cascade ALL doesn't remove orphans
			// but cut database link : memory should mirror this
			assertThat(Iterables.collectToList(persistedCountry.getCities(), City::getCountry)).isEqualTo(Arrays.asList(null, null));
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country " 
					+ "where id = 666");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where " 
					+ "id = 300");
			assertThat(resultSet.next()).isTrue();
		}
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, cities_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country " 
					+ "where id = 42");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from " 
					+ "Country_cities where country_Id = 42");
			assertThat(resultSet.next()).isFalse();
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where " 
					+ "id in (100, 200)");
			assertThat(Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			})).isEqualTo(Arrays.asList(100, 200));
			
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isTrue();
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertThat(resultSet.next()).isFalse();
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where " 
					+ "id = 300");
			assertThat(Iterables.copy(new ResultSetIterator<Object>(resultSet) {
				@Override
				public Object convert(ResultSet resultSet) throws SQLException {
					return resultSet.getObject(1);
				}
			})).isEqualTo(Arrays.asList(300));
		}
		
	}
	
	@Nested
	class CascadeAllOrphanRemoval {
		
		@Test
		void update_mappedBy_removedElementsAreDeleted() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
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
			// Checking deletion has been take into account : the reloaded instance contains cities that are the same as the memory one
			// (comparison are done on equals/hashCode => id)
			assertThat(persistedCountry2.getCities()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
			// Checking update is done too
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void update_associationTable_removedElementsAreDeleted() {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
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
			// Checking deletion has been take into account : the reloaded instance contains cities that are the same as the memory one
			// (comparison are done on equals/hashCode => id)
			assertThat(persistedCountry2.getCities()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
			// Checking update is done too
			assertThat(persistedCountry2.getCities()).extracting(City::getName).containsExactlyInAnyOrder("changed", "Grenoble");
		}
		
		@Test
		void delete_mappedBy() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id, countryId) values (100, 42), (200, 42), (300, 666)");
			
			Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			countryPersister.delete(persistedCountry);
			ResultSet resultSet;
			// Checking that we deleted what we wanted
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertThat(resultSet.next()).isFalse();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertThat(resultSet.next()).isFalse();
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isTrue();
		}
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ALL_ORPHAN_REMOVAL)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, cities_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertThat(resultSet.next()).isFalse();
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertThat(resultSet.next()).isFalse();
			
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isTrue();
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more ensurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertThat(resultSet.next()).isFalse();
			// target entities are not deleted when an association table exists with cascade All
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isFalse();
		}
	}
	
	@Nested
	class CascadeAssociationOnly {
		
		@Test
		void withoutAssociationTable_throwsException() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> mappingBuilder = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					// no cascade
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(ASSOCIATION_ONLY);
			
			assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
					.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage(RelationMode.ASSOCIATION_ONLY + " is only relevant with an association table");
		}
		
		@Test
		void insert_withAssociationTable_associationRecordsMustBeInserted_butNotTargetEntities() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// We need to insert target cities because they won't be inserted by ASSOCIATION_ONLY cascade
			// If they were inserted by cascade an constraint violation error will be thrown 
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			
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
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isTrue();
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertThat(resultSet.next()).isTrue();
		}
		
		@Test
		void update_withAssociationTable_associationRecordsMustBeUpdated_butNotTargetEntities() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we need to insert target cities because they won't be inserted by ASSOCIATION_ONLY cascade
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id) values (100), (200)");
			
			Country country1 = new Country(new PersistableIdentifier<>(42L));
			City city1 = new City(new PersistableIdentifier<>(100L));
			City city2 = new City(new PersistableIdentifier<>(200L));
			country1.addCity(city1);
			country1.addCity(city2);
			
			countryPersister.insert(country1);
			
			// changing values before update
			country1.setName("France");
			city1.setName("Grenoble");
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			
			ResultSet resultSet;
			// Checking that country name was updated
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Country where id = 42");
			ResultSetIterator<Row> countryIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> countryIterator, row -> row.get("name"))).isEqualTo(Arrays.asList("France"));
			// .. but not its city name
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> cityIterator, row -> row.get("name"))).isEqualTo(Arrays.asList((Object) null));
			
			// removing city doesn't have any effect either
			assertThat(country1.getCities().size()).isEqualTo(2);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			country1.getCities().remove(city1);
			assertThat(country1.getCities().size()).isEqualTo(1);	// safeguard for unwanted regression on city removal, because it would totally corrupt this test
			countryPersister.update(country1, countryPersister.select(country1.getId()), true);
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City where id = 100");
			ResultSetIterator<Row> cityIterator2 = new RowIterator(resultSet, Maps.asMap("name", DefaultParameterBinders.STRING_BINDER));
			assertThat(Iterables.collectToList(() -> cityIterator2, row -> row.get("name"))).isEqualTo(Arrays.asList((Object) null));
		}
		
		
		@Test
		void delete_withAssociationTable_associationRecordsMustBeDeleted_butNotTargetEntities() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(ASSOCIATION_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42, 666)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into City(id) values (100), (200), (300)");
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country_cities(country_Id, cities_Id)" +
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
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 42");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 42");
			assertThat(resultSet.next()).isFalse();
			// target entities are deleted when an association table exists
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id in (100, 200)");
			assertThat(resultSet.next()).isTrue();
			// but we didn't delete everything !
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isTrue();
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isTrue();
			
			// testing deletion of the last one
			countryPersister.delete(country2);
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country where id = 666");
			assertThat(resultSet.next()).isFalse();
			// this test is unnecessary because foreign keys should have been violated, left for more insurance
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select * from Country_cities where country_Id = 666");
			assertThat(resultSet.next()).isFalse();
			// target entities are deleted when an association table exists
			resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from City where id = 300");
			assertThat(resultSet.next()).isTrue();
		}
	}
	
	@Nested
	class SelectWithEmptyRelationMustReturnEmptyCollection {
		
		@Test
		void noAssociationTable() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).mappedBy(City::setCountry).cascading(READ_ONLY)
					.build(persistenceContext);
			
			// this is a configuration safeguard, thus we ensure that configuration matches test below
			assertThat(((OptimizedUpdatePersister<Country, Identifier<Long>>) countryPersister).getDelegate()
					.getEntityJoinTree().getJoin("Country_Citys0")).isNull();
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getCities()).isEqualTo(null);
			
		}
		
		@Test
		void withAssociationTable() throws SQLException {
			EntityPersister<Country, Identifier<Long>> countryPersister = MappingEase.entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, CITY_MAPPING_CONFIGURATION).cascading(READ_ONLY)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we only register one country without any city
			persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id) values (42)");
			
			// Then : Country must exist and have an empty city collection
			Country loadedCountry = countryPersister.select(new PersistedIdentifier<>(42L));
			assertThat(loadedCountry.getCities()).isEqualTo(null);
		}
	}
}
