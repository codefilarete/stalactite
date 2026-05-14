package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.FluentMappings;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.JdbcForeignKey;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Bicycle;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;
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
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.tool.collection.Iterables.first;

public class OneToManyResolverTest {
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration;
	private FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		personConfiguration = personMappingBuilder;
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		cityConfiguration = cityMappingBuilder;
	}
	
	@Test
	void multiple_oneToMany_inDepth() {
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName)
				.mapOneToMany(Person::getBicycles, entityBuilder(Bicycle.class, LONG_TYPE)
						.mapSuperClass(entityBuilder(AbstractVehicle.class, LONG_TYPE)
								.mapKey(AbstractVehicle::getId, databaseAutoIncrement()))
						.map(Bicycle::getColor))
				.initializeWith(HashSet::new);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName)
				.mapOneToMany(City::getPersons, personMappingBuilder).initializeWith(HashSet::new);
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterBuilder = FluentMappings.entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.map(Country::getName)
				.map(Country::getDescription)
				.mapOneToMany(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry).cascading(ALL)
				.mapOneToMany(Country::getStates, entityBuilder(State.class, Identifier.LONG_TYPE)
						.mapKey(State::getId, ALREADY_ASSIGNED)
						.map(State::getName));
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterBuilder.getConfiguration());
		
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
		
		LongProvider stateIdProvider = new LongProvider();
		State ileDeFrance = new State(stateIdProvider.giveNewIdentifier());
		ileDeFrance.setName("Ile de France");
		State rhone = new State(stateIdProvider.giveNewIdentifier());
		rhone.setName("Rhône");
		dummyCountry.addState(ileDeFrance);
		dummyCountry.addState(rhone);
		
		LongProvider personIdProvider = new LongProvider();
		Person someone1 = new Person(personIdProvider.giveNewIdentifier());
		someone1.setName("dummy person 1");
		paris.setPersons(Arrays.asHashSet(someone1));
		Bicycle bicycle1 = new Bicycle();
		bicycle1.setColor(new Color(123456));
		someone1.setBicycles(Arrays.asHashSet(bicycle1));
		
		City lyon = new City(cityIdProvider.giveNewIdentifier());
		lyon.setName("Lyon");
		dummyCountry.addCity(lyon);
		
		Person someone2 = new Person(personIdProvider.giveNewIdentifier());
		someone2.setName("dummy person 2");
		lyon.setPersons(Arrays.asHashSet(someone2));
		Bicycle bicycle2 = new Bicycle();
		bicycle2.setColor(new Color(654321));
		someone2.setBicycles(Arrays.asHashSet(bicycle2));
		
		countryPersister.insert(dummyCountry);
		
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		
		ObjectPrinter<Bicycle> colorPrinter = new ObjectPrinterBuilder<Bicycle>()
				.addProperty(Bicycle::getId)
				.addProperty(Bicycle::getColor)
				.withPrinter(Color.class, color -> String.valueOf(color.getRgb()))
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getBicycles, Bicycle.class)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Bicycle.class, colorPrinter::toString)
				.build();
		ObjectPrinter<State> statePrinter = new ObjectPrinterBuilder<State>()
				.addProperty(State::getId)
				.addProperty(State::getName)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
				.addProperty(City::getId)
				.addProperty(City::getName)
				.addProperty(City::getPersons, Person.class)
				.withPrinter(Person.class, personPrinter::toString)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinter<Country> countryPrinter = new ObjectPrinterBuilder<Country>()
				.addProperty(Country::getId)
				.addProperty(Country::getName)
				.addProperty(Country::getStates, State.class)
				.addProperty(Country::getCities, City.class)
				.withPrinter(City.class, cityPrinter::toString)
				.withPrinter(State.class, statePrinter::toString)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		
		assertThat(persistedCountry)
				.usingComparator(Comparator.comparing(countryPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Country.class, countryPrinter))
				.isEqualTo(dummyCountry);
	}
	
	@Nested
	class MappedBy {
		
		@Test
		void foreignKeyIsCreated() throws SQLException {
			// mapping building thanks to fluent API
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class,
					LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, cityConfiguration).mappedBy(City::setCountry).cascading(CascadeOptions.RelationMode.READ_ONLY);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getExportedKeys(null, null,
					"COUNTRY")) {
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
		public void testCascade_oneToOneAndOneToMany_CRUD() {
			// mapping building thanks to fluent API
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
					.mapOneToMany(Country::getCities, cityConfiguration).cascading(ALL).mappedBy(City::setCountry);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
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
			FluentEntityMappingBuilder<State, Identifier<Long>> stateMappingBuilder = entityBuilder(State.class, LONG_TYPE)
					.mapKey(State::getId, ALREADY_ASSIGNED)
					.map(State::getName);
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, cityConfiguration).mappedBy(City::setCountry).cascading(ALL_ORPHAN_REMOVAL)
					.mapOneToMany(Country::getStates, stateMappingBuilder).mappedBy(State::setCountry).cascading(ALL);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
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
	}
	
	@Nested
	class AssociationTable {
		
		@Test
		void foreignKeyIsCreated() throws SQLException {
			// mapping building thanks to fluent API
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class,
					LONG_TYPE)
					// setting a foreign key naming strategy to be tested
					.withForeignKeyNaming(ForeignKeyNamingStrategy.DEFAULT)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, cityConfiguration).cascading(CascadeOptions.RelationMode.READ_ONLY);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			
			ResultSetIterator<JdbcForeignKey> fkCityIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData().getImportedKeys(null, null,
					"COUNTRY_CITIES")) {
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
			JdbcForeignKey expectedForeignKey1 = new JdbcForeignKey("FK_COUNTRY_CITIES_CITIES_ID_CITY_ID", "COUNTRY_CITIES", "CITIES_ID", "CITY", "ID");
			JdbcForeignKey expectedForeignKey2 = new JdbcForeignKey("FK_COUNTRY_CITIES_COUNTRY_ID_COUNTRY_ID", "COUNTRY_CITIES", "COUNTRY_ID", "COUNTRY", "ID");
			assertThat(foundForeignKey).isEqualTo(Arrays.asHashSet(expectedForeignKey1.getSignature(), expectedForeignKey2.getSignature()));
		}
		
		@Test
		public void testCascade_oneToOneAndOneToMany_CRUD() {
			// mapping building thanks to fluent API
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL)
					.mapOneToMany(Country::getCities, cityConfiguration).cascading(ALL);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
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
			FluentEntityMappingBuilder<State, Identifier<Long>> stateMappingBuilder = entityBuilder(State.class, LONG_TYPE)
					.mapKey(State::getId, ALREADY_ASSIGNED)
					.map(State::getName);
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getName)
					.map(Country::getDescription)
					.mapOneToMany(Country::getCities, cityConfiguration).cascading(ALL_ORPHAN_REMOVAL)
					.mapOneToMany(Country::getStates, stateMappingBuilder).cascading(ALL);
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
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
	
	}
	
}
