package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.AbstractRelationConfigurer;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.*;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy.databaseAutoIncrement;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportRelationMixTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	private FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingConfiguration;
	private FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingConfiguration;
	
	@BeforeEach
	public void initTest() {
		// binder creation for our identifier
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");
		
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = FluentMappings.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Person::getName);
		personMappingConfiguration = personMappingBuilder;
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = FluentMappings.entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(City::getName);
		cityMappingConfiguration = cityMappingBuilder;
	}
	
	@Test
	void foreignKeyIsCreated() throws SQLException {
		// mapping building thanks to fluent API
		ConfiguredPersister<Country, Identifier<Long>> countryPersister =
				(ConfiguredPersister<Country, Identifier<Long>>) FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
				// setting a foreign key naming strategy to be tested
				.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personMappingConfiguration)
				.mapOneToMany(Country::getCities, cityMappingConfiguration).mappedBy(City::setCountry).cascading(RelationMode.READ_ONLY)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
		ResultSetIterator<JdbcForeignKey> fkPersonIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null, "PERSON")) {
			@Override
			public JdbcForeignKey convert(ResultSet rs) throws SQLException {
				return new JdbcForeignKey(
						rs.getString("FK_NAME"),
						rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
						rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
				);
			}
		};
		JdbcForeignKey foundForeignKey = first(fkPersonIterator);
		JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_COUNTRY_PRESIDENTID_PERSON_ID", "COUNTRY", "PRESIDENTID", "PERSON", "ID");
		assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		
		ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
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
		foundForeignKey = first(fkCityIterator);
		expectedForeignKey = new JdbcForeignKey("FK_CITY_COUNTRYID_COUNTRY_ID", "CITY", "COUNTRYID", "COUNTRY", "ID");
		assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
	}
	
	@Test
	public void testCascade_oneToOneAndOneToMany_CRUD() {
		// mapping building thanks to fluent API
		EntityPersister<Country, Identifier<Long>> countryPersister = FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.mapOneToOne(Country::getPresident, personMappingConfiguration).cascading(ALL)
				.mapOneToMany(Country::getCities, cityMappingConfiguration).cascading(ALL)
				.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		LongProvider cityIdentifierProvider = new LongProvider();
		City capital = new City(cityIdentifierProvider.giveNewIdentifier());
		capital.setName("Paris");
		dummyCountry.addCity(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(first(persistedCountry.getCities()).getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(first(persistedCountry.getCities()).getId().isPersisted()).isTrue();
		
		// testing update cascade
		persistedCountry.getPresident().setName("New french president");
		City grenoble = new City(cityIdentifierProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		persistedCountry = countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("New french president");
		assertThat(persistedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Grenoble", "Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(first(persistedCountry.getCities()).getId().isPersisted()).isTrue();
	}
	
	
	@Test
	public void testCascade_SetSetMix_update() {
		FluentEntityMappingBuilder<State, Identifier<Long>> stateMappingBuilder = FluentMappings.entityBuilder(State.class, Identifier.LONG_TYPE)
				.mapKey(State::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(State::getName);
		
		EntityPersister<Country, Identifier<Long>> countryPersister = FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.mapOneToMany(Country::getCities, cityMappingConfiguration).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
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
		first(persistedCountry.getCities()).setName("changed");
		
		persistedCountry.getStates().remove(ain);
		State ardeche = new State(new PersistableIdentifier<>(cityIdProvider.giveNewIdentifier()));
		ardeche.setName("ardeche");
		persistedCountry.addState(ardeche);
		first(persistedCountry.getStates()).setName("changed");
		
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		// Checking deletion : for cities we asked for deletion of removed entities so the reloaded instance must have the same content as the memory one
		// but we didn't for regions, so all of them must be there
		// (comparison are done on equals/hashCode => id)
		assertThat(persistedCountry2.getCities()).isEqualTo(Arrays.asHashSet(lyon, grenoble));
		assertThat(persistedCountry2.getStates()).isEqualTo(Arrays.asHashSet(ardeche, isere));
		// Checking update is done too
		assertThat(persistedCountry2.getCities().stream().map(City::getName).collect(toSet())).isEqualTo(Arrays.asHashSet("changed", "Grenoble"));
		assertThat(persistedCountry2.getStates().stream().map(State::getName).collect(toSet())).isEqualTo(Arrays.asHashSet("changed", "ardeche"));
		
		// Ain shouldn't have been deleted because we didn't ask for orphan removal
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from State where id = " + ain.getId().getDelegate(), Long.class)
				.mapKey(Long::new, "id", long.class);
		Set<Long> loadedAin = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(first(loadedAin)).isNotNull();
	}
	
	@Test
	public void testCascade_ListSetMix_listContainsDuplicate_CRUD() {
		FluentEntityMappingBuilder<State, Identifier<Long>> stateMappingBuilder = FluentMappings.entityBuilder(State.class, Identifier.LONG_TYPE)
				.mapKey(State::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(State::getName);
		
		EntityPersister<Country, Identifier<Long>> countryPersister = FluentMappings.entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.mapOneToMany(Country::getAncientCities, cityMappingConfiguration).reverselySetBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL).indexed()
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
		dummyCountry.addAncientCity(paris);
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addAncientCity(lyon);
		// we add a duplicate
		dummyCountry.addAncientCity(lyon);
		
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
		assertThat(persistedCountry).isEqualTo(dummyCountry);
		persistedCountry.getAncientCities().remove(paris);
		City grenoble = new City(cityIdProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addAncientCity(grenoble);
		first(persistedCountry.getAncientCities()).setName("changed");
		
		persistedCountry.getStates().remove(ain);
		State ardeche = new State(new PersistableIdentifier<>(cityIdProvider.giveNewIdentifier()));
		ardeche.setName("ardeche");
		persistedCountry.addState(ardeche);
		first(persistedCountry.getStates()).setName("changed");
		
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		Country persistedCountry2 = countryPersister.select(dummyCountry.getId());
		// Checking deletion : for cities we asked for deletion of removed entities so the reloaded instance must have the same content as the memory one
		// but we didn't for regions, so all of them must be there
		// (comparison are done on equals/hashCode => id)
		assertThat(persistedCountry2.getAncientCities()).isEqualTo(Arrays.asList(lyon, lyon, grenoble));
		assertThat(persistedCountry2.getStates()).isEqualTo(Arrays.asHashSet(ardeche, isere));
		// Checking update is done too
		assertThat(persistedCountry2.getAncientCities().stream().map(City::getName).collect(toSet())).isEqualTo(Arrays.asHashSet("changed", 
				"Grenoble"));
		assertThat(persistedCountry2.getStates().stream().map(State::getName).collect(toSet())).isEqualTo(Arrays.asHashSet("changed", "ardeche"));
		
		// Ain shouldn't have been deleted because we didn't ask for orphan removal
		ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from State where id = " + ain.getId().getDelegate(), Long.class)
				.mapKey(Long::new, "id", long.class);
		Set<Long> loadedAin = longExecutableQuery.execute(Accumulators.toSet());
		assertThat(first(loadedAin)).isNotNull();
	}

	/**
	 * Test cases for persister / configuration reuse.
	 * The initial problem was that the colum and foreign keys on the reused persister were missing: the failing scenario
	 * was due to the call to persistenceContext.build(..) of a configuration (to build a persister) that will be reused
	 * in a relation of another persister.
	 * In depth, the bug was due to the non-lookup of the related / target table in the PersistenceContext.
	 * By looking for it and reusing the table, the problem is fixed
	 * (see {@link AbstractRelationConfigurer#lookupTableInRegisteredPersisters(Class)})
	 */
	@Nested
	class ConfigurationReuse_MappedSuperClassAndRelations {
		
		@Test
		void mappedBy_withOneToOne_foreignKeysAreAddedToSchema() {
			FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> abstractVehicleConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, databaseAutoIncrement());

			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleMappingConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration);
			FluentEntityMappingBuilder<Bicycle, Identifier<Long>> bicycleMappingConfiguration = entityBuilder(Bicycle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration)
					.map(Bicycle::getColor);
			
			FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, databaseAutoIncrement())
					.map(Person::getName)
					// we declare 2 relations on configuration that will be transformed as persisters,
					// and we will expect foreign keys to be created for both of them
					.mapOneToOne(Person::getVehicle, vehicleMappingConfiguration)
					.mappedBy(Vehicle::getOwner)
					.mapOneToMany(Person::getBicycles, bicycleMappingConfiguration)
					.mappedBy(Bicycle::getOwner);

			// we create both persisters to simulate a reuse of the mapping configuration
			vehicleMappingConfiguration.build(persistenceContext);
			bicycleMappingConfiguration.build(persistenceContext);
			personConfiguration.build(persistenceContext);

			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables).extracting(Table::getName).containsExactlyInAnyOrder("Person", "Vehicle", "Bicycle");
			assertThat(tables.stream().flatMap(table -> table.getForeignKeys().stream()))
					.extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_Bicycle_ownerId_Person_id", "FK_Vehicle_ownerId_Person_id");

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			assertThat(ddlDeployer.getCreationScripts()).containsExactly(
					"create table Bicycle(color int, id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"create table Person(name varchar(255), id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Vehicle(id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"alter table Bicycle add constraint FK_Bicycle_ownerId_Person_id foreign key(ownerId) references Person(id)",
					"alter table Vehicle add constraint FK_Vehicle_ownerId_Person_id foreign key(ownerId) references Person(id)"
			);
		}

		@Test
		void mappedBy_withOneToMany_foreignKeysAreAddedToSchema() {
			FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> abstractVehicleConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, databaseAutoIncrement());

			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleMappingConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration);
			FluentEntityMappingBuilder<Bicycle, Identifier<Long>> bicycleMappingConfiguration = entityBuilder(Bicycle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration)
					.map(Bicycle::getColor);

			FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, databaseAutoIncrement())
					.map(Person::getName)
					// we declare 2 relations on configuration that will be transformed as persisters,
					// and we will expect foreign keys to be created for both of them
					.mapOneToOne(Person::getVehicle, vehicleMappingConfiguration)
					.mappedBy(Vehicle::getOwner)
					.mapOneToMany(Person::getBicycles, bicycleMappingConfiguration)
					.mappedBy(Bicycle::getOwner);
			
			// we create both persisters to simulate a reuse of the mapping configuration
			vehicleMappingConfiguration.build(persistenceContext);
			bicycleMappingConfiguration.build(persistenceContext);
			personConfiguration.build(persistenceContext);

			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables).extracting(Table::getName).containsExactlyInAnyOrder("Person", "Vehicle", "Bicycle");
			assertThat(tables.stream().flatMap(table -> table.getForeignKeys().stream()))
					.extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_Bicycle_ownerId_Person_id", "FK_Vehicle_ownerId_Person_id");

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			assertThat(ddlDeployer.getCreationScripts()).containsExactly(
					"create table Bicycle(color int, id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"create table Person(name varchar(255), id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Vehicle(id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"alter table Bicycle add constraint FK_Bicycle_ownerId_Person_id foreign key(ownerId) references Person(id)",
					"alter table Vehicle add constraint FK_Vehicle_ownerId_Person_id foreign key(ownerId) references Person(id)"
			);
		}

		@Test
		void mappedBy_withManyToOne_foreignKeysAreAddedToSchema() {
			FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> abstractVehicleConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, databaseAutoIncrement());

			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleMappingConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration);
			FluentEntityMappingBuilder<Bicycle, Identifier<Long>> bicycleMappingConfiguration = entityBuilder(Bicycle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration)
					.map(Bicycle::getColor);

			FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, databaseAutoIncrement())
					// we declare 2 relations on configuration that will be transformed as persisters,
					// and we will expect foreign keys to be created for both of them
					.map(Person::getName)
					.mapOneToOne(Person::getVehicle, vehicleMappingConfiguration)
					.mappedBy(Vehicle::getOwner)
					.mapManyToOne(Person::getMainBicycle, bicycleMappingConfiguration);
					// no mappedBy here because it's a many-to-one relation

			// we create both persisters to simulate a reuse of the mapping configuration
			vehicleMappingConfiguration.build(persistenceContext);
			bicycleMappingConfiguration.build(persistenceContext);
			personConfiguration.build(persistenceContext);

			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables).extracting(Table::getName).containsExactlyInAnyOrder("Person", "Vehicle", "Bicycle");
			assertThat(tables.stream().flatMap(table -> table.getForeignKeys().stream()))
					.extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_Person_mainBicycleId_Bicycle_id", "FK_Vehicle_ownerId_Person_id");

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			assertThat(ddlDeployer.getCreationScripts()).containsExactly(
					"create table Bicycle(color int, id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Person(name varchar(255), id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, mainBicycleId int, unique (id))",
					"create table Vehicle(id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"alter table Person add constraint FK_Person_mainBicycleId_Bicycle_id foreign key(mainBicycleId) references Bicycle(id)",
					"alter table Vehicle add constraint FK_Vehicle_ownerId_Person_id foreign key(ownerId) references Person(id)"
			);
		}

		@Test
		void mappedBy_withManyToMany_foreignKeysAreAddedToSchema() {
			FluentEntityMappingBuilder<AbstractVehicle, Identifier<Long>> abstractVehicleConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, databaseAutoIncrement());

			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleMappingConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration);
			FluentEntityMappingBuilder<Bicycle, Identifier<Long>> bicycleMappingConfiguration = entityBuilder(Bicycle.class, LONG_TYPE)
					.mapSuperClass(abstractVehicleConfiguration)
					.map(Bicycle::getColor);

			FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, databaseAutoIncrement())
					// we declare 2 relations on configuration that will be transformed as persisters,
					// and we will expect foreign keys to be created for both of them
					.map(Person::getName)
					.mapOneToOne(Person::getVehicle, vehicleMappingConfiguration)
					.mappedBy(Vehicle::getOwner)
					.mapManyToMany(Person::getBicycles, bicycleMappingConfiguration);
					// no mappedBy here because it's a many-to-many relation

			// we create both persisters to simulate a reuse of the mapping configuration
			vehicleMappingConfiguration.build(persistenceContext);
			bicycleMappingConfiguration.build(persistenceContext);
			personConfiguration.build(persistenceContext);

			Collection<Table<?>> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables).extracting(Table::getName).containsExactlyInAnyOrder("Person", "Vehicle", "Person_bicycles", "Bicycle");
			assertThat(tables.stream().flatMap(table -> table.getForeignKeys().stream()))
					.extracting(ForeignKey::getName).containsExactlyInAnyOrder(
							"FK_Person_bicycles_person_id_Person_id",
							"FK_Person_bicycles_bicycles_id_Bicycle_id",
							"FK_Vehicle_ownerId_Person_id");

			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			assertThat(ddlDeployer.getCreationScripts()).containsExactlyInAnyOrder(
					"create table Bicycle(color int, id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Person(name varchar(255), id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, unique (id))",
					"create table Person_bicycles(person_id int not null, bicycles_id int not null, unique (person_id, bicycles_id))",
					"create table Vehicle(id int GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) not null, ownerId int, unique (id))",
					"alter table Person_bicycles add constraint FK_Person_bicycles_person_id_Person_id foreign key(person_id) references Person(id)",
					"alter table Person_bicycles add constraint FK_Person_bicycles_bicycles_id_Bicycle_id foreign key(bicycles_id) references Bicycle(id)",
					"alter table Vehicle add constraint FK_Vehicle_ownerId_Person_id foreign key(ownerId) references Person(id)"
			);
		}
	}
}
