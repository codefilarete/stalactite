package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Set;
import javax.sql.DataSource;

import org.codefilarete.stalactite.dsl.RuntimeMappingException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.ExecutableQuery;
import org.codefilarete.stalactite.engine.FluentEntityMappingConfigurationSupportOneToOneTest;
import org.codefilarete.stalactite.engine.PartialRepresentation;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.State;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.hsqldb.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.hsqldb.test.HSQLDBInMemoryDataSource;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

class OneToOneResolverTest {
	
	@Test
	void multiple_oneToOne() throws SQLException {
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		
		persistenceContext.getDialect().getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		persistenceContext.getDialect().getSqlTypeRegistry().put(Identifier.class, "int");
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personMappingBuilder).cascading(ALL)
				.mapOneToOne(Country::getCapital, cityMappingBuilder).cascading(ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
		
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
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getDelegate()).isEqualTo(persistedCountry.getPresident().getId().getDelegate());
		assertThat(persistedCountry2.getCapital().getId().getDelegate()).isEqualTo(persistedCountry.getCapital().getId().getDelegate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("French president renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getDelegate())).isEqualTo(1);
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
	@Test
	void multiple_oneToOne_inDepth() throws SQLException {
		
		DataSource inMemoryDataSource = new HSQLDBInMemoryDataSource();
		PersistenceContext persistenceContext = new PersistenceContext(inMemoryDataSource);
		
		persistenceContext.getDialect().getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		persistenceContext.getDialect().getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext.getDialect().getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		persistenceContext.getDialect().getSqlTypeRegistry().put(Color.class, "int");
		
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleBuilder = entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.map(Vehicle::getColor);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleBuilder)
				.map(Person::getName);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.mapOneToOne(City::getState, entityBuilder(State.class, Identifier.LONG_TYPE)
						.mapKey(State::getId, ALREADY_ASSIGNED)
						.map(State::getName))
				.map(City::getName);
		
		
		FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.map(Country::getDescription)
				.mapOneToOne(Country::getPresident, personMappingBuilder).cascading(ALL)
				.mapOneToOne(Country::getCapital, cityMappingBuilder).cascading(ALL);
		
		AggregateResolver testInstance = new AggregateResolver(persistenceContext);
		EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		LongProvider countryIdProvider = new LongProvider();
		Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry.setName("France");
		dummyCountry.setDescription("Smelly cheese !");
		
		Person person = new Person(new LongProvider().giveNewIdentifier());
		person.setName("French president");
		dummyCountry.setPresident(person);
		
		Car car = new Car(42L);
		car.setColor(new Color(17));
		person.setVehicle(car);
		
		City capital = new City(new LongProvider().giveNewIdentifier());
		capital.setName("Paris");
		State state = new State(new LongProvider().giveNewIdentifier());
		state.setName("Ile de France");
		capital.setState(state);
		dummyCountry.setCapital(capital);
		
		// testing insert cascade
		countryPersister.insert(dummyCountry);
		Country persistedCountry = countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getCapital().getState().getName()).isEqualTo("Ile de France");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
		assertThat(persistedCountry.getCapital().getState().getId().isPersisted()).isTrue();
		
		// testing insert cascade with another Country reusing OneToOne entities
		Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
		dummyCountry2.setName("France 2");
		dummyCountry2.setPresident(person);
		dummyCountry2.setCapital(capital);
		countryPersister.insert(dummyCountry2);
		// database must be up to date
		Country persistedCountry2 = countryPersister.select(dummyCountry2.getId());
		assertThat(persistedCountry2.getId()).isEqualTo(new PersistedIdentifier<>(1L));
		assertThat(persistedCountry2.getPresident().getName()).isEqualTo("French president");
		assertThat(persistedCountry2.getPresident().getId().getDelegate()).isEqualTo(persistedCountry.getPresident().getId().getDelegate());
		assertThat(persistedCountry2.getCapital().getId().getDelegate()).isEqualTo(persistedCountry.getCapital().getId().getDelegate());
		assertThat(persistedCountry2.getPresident()).isNotSameAs(persistedCountry.getPresident());
		assertThat(persistedCountry2.getCapital()).isNotSameAs(persistedCountry.getCapital());
		assertThat(persistedCountry2.getCapital().getState()).isNotSameAs(persistedCountry.getCapital().getState());
		
		// testing update cascade
		persistedCountry2.getPresident().setName("French president renamed");
		persistedCountry2.getCapital().setName("Paris renamed");
		persistedCountry2.getCapital().getState().setName("Ile de France renamed");
		countryPersister.update(persistedCountry2, dummyCountry2, true);
		// database must be up to date
		ResultSet resultSet;
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from Person");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("French president renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from City");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Paris renamed");
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select name from State");
		resultSet.next();
		assertThat(resultSet.getString("name")).isEqualTo("Ile de France renamed");
		assertThat(resultSet.next()).isFalse();
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from enough for our checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursion 
		ObjectPrinterBuilder.ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.build();
		ObjectPrinterBuilder.ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getDelegate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		Person loadedPerson = persistedCountry2.getPresident();
		
		Person expectedPerson = new Person(person.getId());
		expectedPerson.setName("French president renamed");
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(expectedPerson);
		// ensuring that reverse side is also set
		assertThat(loadedPerson.getVehicle().getOwner()).isEqualTo(loadedPerson);
		
		// testing delete cascade
		// but we have to remove first the other country that points to the same president, else will get a constraint violation
		assertThat(persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate(
				"update Country set presidentId = null, capitalId = null where id = " + dummyCountry2.getId().getDelegate())).isEqualTo(1);
		countryPersister.delete(persistedCountry);
		// database must be up to date
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Country where id = " + persistedCountry.getId().getDelegate());
		assertThat(resultSet.next()).isFalse();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from Person where id = " + persistedCountry.getPresident().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from City where id = " + persistedCountry.getCapital().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
		resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement()
				.executeQuery("select id from State where id = " + persistedCountry.getCapital().getState().getId().getDelegate());
		assertThat(resultSet.next()).isTrue();
	}
	
	private final Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private FluentEntityMappingBuilder<Person, Identifier<Long>> personConfiguration;
	private FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration;
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		persistenceContext = new PersistenceContext(dataSource, dialect);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		personConfiguration = personMappingBuilder;
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		cityConfiguration = cityMappingBuilder;
	}
	
	@Nested
	class CascadeAll {
		
		@Test
		void ownedBySourceSide() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry);
			
			assertCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideGetter() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::getCountry);
			
			assertCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideSetter() {
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfiguration).cascading(ALL).mappedBy(City::setCountry);
			
			assertCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideColumn() {
			Table cityTable = new Table("City");
			Column countryId = cityTable.addColumn("countryId", Identifier.LONG_TYPE);
			
			EntityMappingConfigurationProvider<City, Identifier<Long>> cityConfigurer = entityBuilder(City.class, Identifier.LONG_TYPE)
					.onTable(cityTable)
					.mapKey(City::getId, ALREADY_ASSIGNED)
					.map(City::getName);
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfigurer).cascading(ALL).reverseJoinColumn(countryId);
			
			assertCascadeAll(countryPersister);
		}
		
		@Test
		void ownedByReverseSideColumnName() {
			EntityMappingConfigurationProvider<City, Identifier<Long>> cityConfigurer = entityBuilder(City.class, Identifier.LONG_TYPE)
					.mapKey(City::getId, ALREADY_ASSIGNED)
					.map(City::getName);
			
			FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersister = entityBuilder(Country.class, Identifier.LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.map(Country::getDescription)
					.mapOneToOne(Country::getCapital, cityConfigurer).cascading(ALL).reverseJoinColumn("countryId");
			
			assertCascadeAll(countryPersister);
		}
		
		/**
		 * Common tests of cascade-all with different owner definition.
		 * Should have been done with a @ParameterizedTest but can't be done in such a way due to database commit between tests and cityPersister
		 * dependency
		 */
		private void assertCascadeAll(FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration) {
			
			AggregateResolver testInstance = new AggregateResolver(persistenceContext);
			EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			LongProvider countryIdProvider = new LongProvider(42);
			Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
			dummyCountry.setName("France");
			dummyCountry.setDescription("Smelly cheese !");
			
			LongProvider cityIdProvider = new LongProvider();
			City paris = new City(cityIdProvider.giveNewIdentifier());
			paris.setName("Paris");
			dummyCountry.setCapital(paris);
			
			// insert cascade test
			countryPersister.insert(dummyCountry);
			Country persistedCountry = countryPersister.select(dummyCountry.getId());
			assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(42L));
			assertThat(persistedCountry.getDescription()).isEqualTo("Smelly cheese !");
			assertThat(persistedCountry.getCapital().getName()).isEqualTo("Paris");
			assertThat(persistedCountry.getCapital().getId().isPersisted()).isTrue();
			
			// choosing better names for next tests
			Country modifiedCountry = persistedCountry;
			Country referentCountry = dummyCountry;
			
			// nullifiying relation test
			modifiedCountry.setCapital(null);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertThat(modifiedCountry.getCapital()).isNull();
			// ensuring that capital was not deleted nor updated (we didn't ask for orphan removal)
			
			PersistenceContext.ExecutableBeanPropertyQueryMapper<FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity> citySelector = persistenceContext.newQuery("select name, countryId from City where id = :id", FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity.class)
					.mapKey(FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity::new, "name", String.class, "countryId", Integer.class);
			FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity city = citySelector
					.set("id", paris.getId())
					.execute(Accumulators.getFirstUnique());
			// but relation is cut on both sides (because setCapital(..) calls setCountry(..))
			assertThat(city).usingRecursiveComparison().isEqualTo(new FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity("Paris", null));
			
			// from null to a (new) object
			referentCountry = countryPersister.select(referentCountry.getId());
			City lyon = new City(cityIdProvider.giveNewIdentifier());
			lyon.setName("Lyon");
			modifiedCountry.setCapital(lyon);
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			assertThat(modifiedCountry.getCapital()).isEqualTo(lyon);
			// ensuring that capital was not deleted nor updated
			assertThat(citySelector
					.set("id", lyon.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity("Lyon", 42));
			
			// testing update cascade
			referentCountry = countryPersister.select(referentCountry.getId());
			modifiedCountry.getCapital().setName("Lyon renamed");
			countryPersister.update(modifiedCountry, referentCountry, false);
			modifiedCountry = countryPersister.select(referentCountry.getId());
			// ensuring that capital was not deleted nor updated
			assertThat(citySelector
					.set("id", lyon.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity("Lyon renamed", 42));
			
			// testing delete cascade
			countryPersister.delete(modifiedCountry);
			// ensuring that capital was not deleted nor updated
			assertThat(citySelector
					.set("id", lyon.getId())
					.execute(Accumulators.getFirstUnique())).usingRecursiveComparison().isEqualTo(new FluentEntityMappingConfigurationSupportOneToOneTest.LiteCity("Lyon renamed", null));
		}
		
		@Nested
		class Insert {
			
			@Test
			void insertOnce_targetInstanceIsInserted() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
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
				assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
				assertThat(persistedCountry.getDescription()).isEqualTo("Smelly cheese !");
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
			}
			
			@Test
			void insertTwice_targetInstanceIsInsertedOnce() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
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
				
				// Creating a new country with the same president (!): the president shouldn't be resaved
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				countryPersister.insert(dummyCountry2);
				
				// Checking that the country is persisted but not the president since it has been previously
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(1L));
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
				assertThat(persistedCountry.getPresident().getId().getDelegate()).isEqualTo(dummyCountry.getPresident().getId().getDelegate());
				// President is cloned since we did nothing during select to reuse the existing one
				assertThat(persistedCountry.getPresident()).isNotSameAs(dummyCountry.getPresident());
			}
			
			@Test
			void mandatory_withNullTarget_throwsException() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory();
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				assertThatCode(() -> countryPersister.insert(dummyCountry))
						.isInstanceOf(RuntimeMappingException.class)
						.hasMessageStartingWith("Non null value expected for relation Country::getPresident on object Country");
			}
			
			@Test
			void insert_targetInstanceIsUpdated() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
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
				
				// Creating a new country with the same president (!) and changing president
				person.setName("Me !!");
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				countryPersister.insert(dummyCountry2);
				
				// Checking that president is modified
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("Me !!");
				// ... and we still a 2 countries (no deletion was done)
				ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select count(*) as countryCount from Country", Long.class)
						.mapKey(Long::new, "countryCount", Long.class);
				Set<Long> countryCount = longExecutableQuery.execute(Accumulators.toSet());
				assertThat(Iterables.first(countryCount)).isEqualTo(2);
			}
			
			@Test
			void insert_targetInstanceIsUpdated_ownedByReverseSide() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mappedBy(Person::getCountry);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person person = new Person(new LongProvider().giveNewIdentifier());
				person.setName("French president");
				dummyCountry.setPresident(person);
				person.setCountry(dummyCountry);
				countryPersister.insert(dummyCountry);
				
				// Creating a new country with the same president (!) and modifying president
				person.setName("Me !!");
				Country dummyCountry2 = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry2.setName("France 2");
				dummyCountry2.setPresident(person);
				person.setCountry(dummyCountry2);
				countryPersister.insert(dummyCountry2);
				
				// Checking that president is modified
				Country persistedCountry = countryPersister.select(dummyCountry2.getId());
				assertThat(persistedCountry.getPresident().getName()).isEqualTo("Me !!");
				// ... and we still have 2 countries (no deletion was done)
				ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select count(*) as countryCount from Country", Long.class)
						.mapKey(Long::new, "countryCount", Long.class);
				Set<Long> countryCount = longExecutableQuery.execute(Accumulators.toSet());
				assertThat(Iterables.first(countryCount)).isEqualTo(2);
			}
		}
		
		@Nested
		class Update {
			
			@Test
			void relationChanged_relationIsOwnedBySource() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				LongProvider personIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person originalPresident = new Person(personIdProvider.giveNewIdentifier());
				originalPresident.setName("French president");
				dummyCountry.setPresident(originalPresident);
				countryPersister.insert(dummyCountry);
				
				// Changing president's name to see what happens when we save it to the database
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.getPresident().setName("French president renamed");
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that changing president's name is pushed to the database when we save the country
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("French president renamed");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
				
				// Changing president
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("new French president");
				persistedCountry.setPresident(newPresident);
				countryPersister.update(persistedCountry, countryFromDB, true);
				// Checking that president has changed
				countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("new French president");
				assertThat(countryFromDB.getPresident().getId()).isEqualTo(newPresident.getId());
				// and original one was left untouched
				EntityPersister<Person, Identifier<Long>> personPersister = personConfiguration.build(persistenceContext);
				assertThat(personPersister.select(originalPresident.getId()).getName()).isEqualTo("French president renamed");
			}
			
			@Test
			void relationChanged_relationIsOwnedByTarget() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mappedBy(Person::getCountry);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				LongProvider personIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person originalPresident = new Person(personIdProvider.giveNewIdentifier());
				originalPresident.setName("French president");
				dummyCountry.setPresident(originalPresident);
				originalPresident.setCountry(dummyCountry);	// maintaining reverse relation in memory (as it must be), else column has not value which results in an NPE
				countryPersister.insert(dummyCountry);
				
				// Changing president's name to see what happens when we save it to the database
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.getPresident().setName("French president renamed");
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that changing president's name is pushed to the database when we save the country
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("French president renamed");
				assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
				
				// Changing president
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("new French president");
				persistedCountry.setPresident(newPresident);
				newPresident.setCountry(dummyCountry);	// maintaining reverse relation in memory (as it must be), else column has not value which results in an NPE
				countryPersister.update(persistedCountry, countryFromDB, true);
				// Checking that president has changed
				countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident().getName()).isEqualTo("new French president");
				assertThat(countryFromDB.getPresident().getId()).isEqualTo(newPresident.getId());
				// and original one was left untouched
				EntityPersister<Person, Identifier<Long>> personPersister = personConfiguration.build(persistenceContext);
				assertThat(personPersister.select(originalPresident.getId()).getName()).isEqualTo("French president renamed");
				
				// checking reverse side column value ...
				// ... must be null for old president
				PersistenceContext.ExecutableBeanPropertyQueryMapper<Long> countryIdQuery = persistenceContext.newQuery("select countryId from Person where id = :personId", Long.class)
						.mapKey("countryId", Long.class);
				ExecutableQuery<Long> longExecutableQuery1 = countryIdQuery
						.set("personId", originalPresident.getId());
				Set<Long> originalPresidentCountryId = longExecutableQuery1.execute(Accumulators.toSet());
				assertThat(Iterables.first(originalPresidentCountryId)).isNull();
				// ... and not null for new president
				ExecutableQuery<Long> longExecutableQuery = countryIdQuery
						.set("personId", newPresident.getId());
				Set<Long> newPresidentCountryId = longExecutableQuery.execute(Accumulators.toSet());
				assertThat(dummyCountry.getId().getDelegate()).isEqualTo(Iterables.first(newPresidentCountryId));
			}
			
			@Test
			void relationNullified_relationIsOwnedBySource() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				LongProvider personIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person president = new Person(personIdProvider.giveNewIdentifier());
				president.setName("French president");
				dummyCountry.setPresident(president);
				countryPersister.insert(dummyCountry);
				
				// Removing president
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.setPresident(null);
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that president is no more related to country
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident()).isNull();
				// President shouldn't be deleted because orphan removal wasn't asked
				EntityPersister<Person, Identifier<Long>> personPersister = personConfiguration.build(persistenceContext);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNotNull();
				// properties shouldn't have been nullified
				assertThat(previousPresident.getName()).isNotNull();
			}
			
			@Test
			void relationNullifiedWithOrphanRemoval() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				LongProvider personIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person president = new Person(personIdProvider.giveNewIdentifier());
				president.setName("French president");
				dummyCountry.setPresident(president);
				countryPersister.insert(dummyCountry);
				
				// Removing president
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.setPresident(null);
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that president has changed
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident()).isNull();
				// previous president has been deleted
				EntityPersister<Person, Identifier<Long>> personPersister = personConfiguration.build(persistenceContext);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNull();
			}
			
			@Test
			void relationChanged_relationIsOwnedBySource_withOrphanRemoval() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL_ORPHAN_REMOVAL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				LongProvider countryIdProvider = new LongProvider();
				LongProvider personIdProvider = new LongProvider();
				Country dummyCountry = new Country(countryIdProvider.giveNewIdentifier());
				dummyCountry.setName("France");
				dummyCountry.setDescription("Smelly cheese !");
				Person president = new Person(personIdProvider.giveNewIdentifier());
				president.setName("French president");
				dummyCountry.setPresident(president);
				countryPersister.insert(dummyCountry);
				
				// Removing president
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				Person newPresident = new Person(personIdProvider.giveNewIdentifier());
				newPresident.setName("New French president");
				persistedCountry.setPresident(newPresident);
				countryPersister.update(persistedCountry, dummyCountry, true);
				// Checking that president has changed
				Country countryFromDB = countryPersister.select(dummyCountry.getId());
				assertThat(countryFromDB.getPresident()).isEqualTo(newPresident);
				// previous president has been deleted
				EntityPersister<Person, Identifier<Long>> personPersister = personConfiguration.build(persistenceContext);
				Person previousPresident = personPersister.select(president.getId());
				assertThat(previousPresident).isNull();
			}
			
			@Test
			void mandatory_withNullTarget_throwsException() {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getName)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL).mandatory();
				
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
				countryPersister.insert(dummyCountry);
				
				// Changing president's name to see what happens when we save it to the database
				Country persistedCountry = countryPersister.select(dummyCountry.getId());
				persistedCountry.setPresident(null);
				assertThatCode(() -> countryPersister.update(persistedCountry, dummyCountry, true))
						.isInstanceOf(RuntimeMappingException.class)
						.hasMessageStartingWith("Non null value expected for relation Country::getPresident on object Country");
			}
			
		}
		
		@Nested
		class Delete {
			
			@Test
			void targetEntityIsDeleted() throws SQLException {
				FluentEntityMappingBuilder<Country, Identifier<Long>> countryPersisterConfiguration = entityBuilder(Country.class, Identifier.LONG_TYPE)
						.mapKey(Country::getId, ALREADY_ASSIGNED)
						.map(Country::getDescription)
						.mapOneToOne(Country::getPresident, personConfiguration).cascading(ALL);
				
				AggregateResolver testInstance = new AggregateResolver(persistenceContext);
				EntityPersister<Country, Identifier<Long>> countryPersister = testInstance.resolve(countryPersisterConfiguration.getConfiguration());
				
				DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
				ddlDeployer.deployDDL();
				
				persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Person(id) values (42), (666)");
				persistenceContext.getConnectionProvider().giveConnection().createStatement().executeUpdate("insert into Country(id, presidentId) values (100, 42), (200, 666)");
				
				Country persistedCountry = countryPersister.select(new PersistedIdentifier<>(100L));
				countryPersister.delete(persistedCountry);
				ResultSet resultSet;
				// Checking that we deleted what we wanted
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country"
						+ " where id = 100");
				assertThat(resultSet.next()).isFalse();
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person"
						+ " where id = 42");
				assertThat(resultSet.next()).isTrue();
				// but we didn't delete everything !
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Country"
						+ " where id = 200");
				assertThat(resultSet.next()).isTrue();
				resultSet = persistenceContext.getConnectionProvider().giveConnection().createStatement().executeQuery("select id from Person where id = 666");
				assertThat(resultSet.next()).isTrue();
			}
		}
	}
	
}
