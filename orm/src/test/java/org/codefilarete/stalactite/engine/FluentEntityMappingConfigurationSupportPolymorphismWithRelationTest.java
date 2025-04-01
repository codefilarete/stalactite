package org.codefilarete.stalactite.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderOneToManyOptions;
import org.codefilarete.stalactite.engine.FluentEntityMappingBuilder.FluentMappingBuilderOneToOneOptions;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.engine.idprovider.LongProvider;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Car.Radio;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.Engine;
import org.codefilarete.stalactite.engine.model.Person;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.model.Town;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.engine.model.Vehicle.Wheel;
import org.codefilarete.stalactite.engine.model.Village;
import org.codefilarete.stalactite.id.AbstractIdentifier;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.ResultSetIterator;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.trace.ObjectPrinterBuilder;
import org.codefilarete.trace.ObjectPrinterBuilder.ObjectPrinter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy.alreadyAssigned;
import static org.codefilarete.stalactite.engine.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportPolymorphismWithRelationTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
	}
	
	@BeforeEach
	public void beforeTest() {
		persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
	}
	
	
	static Object[][] polymorphicOneToOne_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		Object[][] result = new Object[][] {
				{	"single table",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
								.mapKey(Engine::getId, ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel), "CAR")
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor), "TRUCK"))
						.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"joined tables",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
								.mapKey(Engine::getId, ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor)))
						.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"table per class",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
									.mapKey(Engine::getId, ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor)))
						.build(persistenceContext3),
						persistenceContext3.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	} 
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphicOneToOne_data")
	void crudPolymorphicOneToOne(String testDisplayName, EntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.setEngine(new Engine(100L));
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
		dummyCarModified.setEngine(new Engine(200L));
		Truck dummyTruckModified = new Truck(2L);
		dummyTruckModified.setColor(new Color(99));
		
		persister.update(dummyCarModified, dummyCar, true);
		
		persister.update(dummyTruckModified, dummyTruck, true);
		
		connectionProvider.giveConnection().commit();
		persister.delete(dummyCarModified);
		persister.delete(dummyTruckModified);
		connectionProvider.giveConnection().rollback();
		
		persister.delete(Arrays.asList(dummyCarModified, dummyTruckModified));
		
		connectionProvider.giveConnection().rollback();
		
		assertThat(persister.select(dummyTruck.getId())).isEqualTo(dummyTruckModified);
		assertThat(persister.select(dummyCar.getId())).isEqualTo(dummyCarModified);
		assertThat(new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruck.getId())))).isEqualTo(Arrays.asSet(dummyCarModified,
				dummyTruckModified));
	}
	
	static Object[][] polymorphism_trunkHasOneToMany_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		
		FluentEntityMappingBuilder<Person, Identifier<Long>> personMappingBuilder = MappingEase.entityBuilder(Person.class, Identifier.LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.map(Person::getName);
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class, Identifier.LONG_TYPE)
				.mapKey(City::getId, ALREADY_ASSIGNED)
				.map(City::getName);
		
		Object[][] result = new Object[][] {
				{	"single table",
						entityBuilder(Country.class, Identifier.LONG_TYPE)
								.mapKey(Country::getId, ALREADY_ASSIGNED)
								.map(Country::getName)
								.map(Country::getDescription)
								.mapOneToOne(Country::getPresident, personMappingBuilder)
								.mapOneToMany(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry)
								.mapPolymorphism(PolymorphismPolicy.<Country>singleTable()
										.addSubClass(subentityBuilder(Republic.class)
												.map(Republic::getDeputeCount), "Republic"))
								.build(persistenceContext1)
				},
				{	"joined tables",
						entityBuilder(Country.class, Identifier.LONG_TYPE)
								.mapKey(Country::getId, ALREADY_ASSIGNED)
								.map(Country::getName)
								.map(Country::getDescription)
								.mapOneToOne(Country::getPresident, personMappingBuilder)
								.mapOneToMany(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry)
								.mapPolymorphism(PolymorphismPolicy.<Country>joinTable()
										.addSubClass(subentityBuilder(Republic.class)
												.map(Republic::getDeputeCount)))
								.build(persistenceContext2)
				},
				// Not implementable : one-to-many with mappedby targeting a table-per-class polymorphism is not implemented due to fk constraint, how to ? forget foreign key ?
//				{	"table per class",
//						entityBuilder(Country.class, Identifier.LONG_TYPE)
//								.map(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
//								.map(Country::getName)
//								.map(Country::getDescription)
//								.mapOneToOne(Country::getPresident, personMappingBuilder)
//								.mapOneToManySet(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry).cascading(RelationMode.READ_ONLY)
//								.mapPolymorphism(PolymorphismPolicy.<Republic>tablePerClass()
//										.addSubClass(subentityBuilder(Republic.class)
//												.map(Republic::getDeputeCount)))
//								.build(persistenceContext3)
//				 },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_trunkHasOneToMany_data")
	void crud_polymorphism_trunkHasOneToMany(String testDisplayName, EntityPersister<Country, Identifier<Long>> countryPersister) {
		LongProvider countryIdProvider = new LongProvider();
		Republic dummyCountry = new Republic(new PersistableIdentifier<>(countryIdProvider.giveNewIdentifier()));
		dummyCountry.setDeputeCount(250);
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
		Republic persistedCountry = (Republic) countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("French president");
		assertThat(Iterables.first(persistedCountry.getCities()).getName()).isEqualTo("Paris");
		assertThat(persistedCountry.getDeputeCount()).isEqualTo(250);
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(Iterables.first(persistedCountry.getCities()).getId().isPersisted()).isTrue();
		
		// testing update cascade
		persistedCountry.getPresident().setName("New french president");
		City grenoble = new City(cityIdentifierProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		persistedCountry = (Republic) countryPersister.select(dummyCountry.getId());
		assertThat(persistedCountry.getId()).isEqualTo(new PersistedIdentifier<>(0L));
		assertThat(persistedCountry.getPresident().getName()).isEqualTo("New french president");
		assertThat(persistedCountry.getCities()).extracting(City::getName).containsExactlyInAnyOrder("Grenoble", "Paris");
		assertThat(persistedCountry.getPresident().getId().isPersisted()).isTrue();
		assertThat(Iterables.first(persistedCountry.getCities()).getId().isPersisted()).isTrue();
	}
	
	static Object[][] polymorphism_subClassHasOneToOne_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		Object[][] result = new Object[][] {
					{	"single table / one-to-one with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.mapKey(Engine::getId, ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToOne(Car::getRadio, entityBuilder(Radio.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Radio::getSerialNumber, alreadyAssigned(Radio::markAsPersisted, Radio::isPersisted))
														.map(Radio::getModel)).mappedBy(Radio::getCar), "CAR")
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor), "TRUCK"))
								.build(persistenceContext1),
							persistenceContext1.getConnectionProvider() },
				{	"joined tables / one-to-one with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToOne(Car::getRadio, entityBuilder(Radio.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Radio::getSerialNumber, alreadyAssigned(Radio::markAsPersisted, Radio::isPersisted))
														.map(Radio::getModel)).mappedBy(Radio::getCar))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"table per class / one-to-one with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToOne(Car::getRadio, entityBuilder(Radio.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Radio::getSerialNumber, alreadyAssigned(Radio::markAsPersisted, Radio::isPersisted))
														.map(Radio::getModel)).mappedBy(Radio::getCar))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext3),
						persistenceContext3.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_subClassHasOneToOne_data")
	void crud_polymorphism_subClassHasOneToOne(String testDisplayName, EntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.setRadio(new Radio("XYZ-ABC-01"));
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
		dummyCarModified.setRadio(new Radio("XYZ-ABC-02"));
		Truck dummyTruckModified = new Truck(2L);
		dummyTruckModified.setColor(new Color(99));
		
		persister.update(dummyCarModified, dummyCar, true);
		
		persister.update(dummyTruckModified, dummyTruck, true);
		
		connectionProvider.giveConnection().commit();
		persister.delete(dummyCarModified);
		persister.delete(dummyTruckModified);
		connectionProvider.giveConnection().rollback();
		
		persister.delete(Arrays.asList(dummyCarModified, dummyTruckModified));
		
		connectionProvider.giveConnection().rollback();
		
		assertThat(persister.select(dummyTruck.getId())).isEqualTo(dummyTruckModified);
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		assertThat(selectedCar).isEqualTo(dummyCarModified);
		assertThat(((Car) selectedCar).getRadio().isPersisted()).isTrue();	// testing afterSelect listener of sub entities relations 
		assertThat(((Car) selectedCar).getRadio()).isEqualTo(dummyCarModified.getRadio());
		assertThat(new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruck.getId())))).isEqualTo(Arrays.asSet(dummyCarModified,
				dummyTruckModified));
	}
	
	static Object[][] polymorphism_subClassHasOneToMany_data() {
		// each test has each own context so they can't pollute each other
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext4 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext5 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext6 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext7 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		Table wheelTable1 = new Table("Wheel");
		Column<Table, Integer> indexColumn1 = wheelTable1.addColumn("idx", Integer.class);
		Table wheelTable2 = new Table("Wheel");
		Column<Table, Integer> indexColumn2 = wheelTable2.addColumn("idx", Integer.class);
		Table wheelTable3 = new Table("Wheel");
		Column<Table, Integer> indexColumn3 = wheelTable3.addColumn("idx", Integer.class);
		Table wheelTable4 = new Table("Wheel");
		Column<Table, Integer> indexColumn4 = wheelTable4.addColumn("idx", Integer.class);
		Table wheelTable5 = new Table("Wheel");
		Column<Table, Integer> indexColumn5 = wheelTable5.addColumn("idx", Integer.class);
		Object[][] result = new Object[][] {
				{	"single table / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.mapKey(Engine::getId, ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).reverselySetBy(Wheel::setVehicle), "CAR")
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor), "TRUCK"))
								.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"single table / one-to-many with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.mapKey(Engine::getId, ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).indexedBy(indexColumn1).mappedBy(Wheel::setVehicle), "CAR")
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor), "TRUCK"))
								.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"joined tables / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).reverselySetBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext3),
						persistenceContext3.getConnectionProvider() },
				{	"joined tables / one-to-many with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).indexedBy(indexColumn2).mappedBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext4),
						persistenceContext4.getConnectionProvider() },
				{	"joined tables / one-to-many with mapped association / association is defined as common property",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapOneToMany(Vehicle::getWheels, entityBuilder(Wheel.class, String.class)
										// please note that we use an already-assigned policy because it requires entities to be marked
										// as persisted after select, so we test also select listener of relation
										.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
										.map(Wheel::getModel))
									.indexedBy(indexColumn4).mappedBy(Wheel::setVehicle)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext5),
						persistenceContext5.getConnectionProvider() },
				{	"table per class / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).reverselySetBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext6),
						persistenceContext6.getConnectionProvider() },
				{	"table per class / one-to-many with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
										.addSubClass(subentityBuilder(Car.class)
												.map(Car::getModel)
												.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be marked
														// as persisted after select, so we test also select listener of relation
														.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.map(Wheel::getModel)).indexedBy(indexColumn5).mappedBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truck.class)
												.map(Truck::getColor)))
								.build(persistenceContext7),
						persistenceContext7.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		new DDLDeployer(persistenceContext4).deployDDL();
		new DDLDeployer(persistenceContext5).deployDDL();
		new DDLDeployer(persistenceContext6).deployDDL();
		new DDLDeployer(persistenceContext7).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_subClassHasOneToMany_data")
	void crud_polymorphism_subClassHasOneToMany(String testDisplayName, EntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.addWheel(new Wheel("XYZ-ABC-01"));
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
		dummyCarModified.addWheel(new Wheel("XYZ-ABC-02"));
		Truck dummyTruckModified = new Truck(2L);
		dummyTruckModified.setColor(new Color(99));
		
		persister.update(dummyCarModified, dummyCar, true);
		
		persister.update(dummyTruckModified, dummyTruck, true);
		
		connectionProvider.giveConnection().commit();
		persister.delete(dummyCarModified);
		persister.delete(dummyTruckModified);
		connectionProvider.giveConnection().rollback();
		
		persister.delete(Arrays.asList(dummyCarModified, dummyTruckModified));
		
		connectionProvider.giveConnection().rollback();
		
		assertThat(persister.select(dummyTruck.getId())).isEqualTo(dummyTruckModified);
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		dummyCarModified.getWheels().forEach(w -> w.setVehicle(dummyCarModified));	// this is done only for equality check of reverse setting, because deletion set it to null (which must be fixed, bug see CollecctionUpdater)
		assertThat(selectedCar).isEqualTo(dummyCarModified);
		// testing afterSelect listener of sub entities relations
		((Car) selectedCar).getWheels().forEach(wheel -> assertThat(wheel.isPersisted()).isTrue());
			 
		assertThat(((Car) selectedCar).getWheels()).isEqualTo(dummyCarModified.getWheels());
		assertThat(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruck.getId()))).containsExactlyInAnyOrder(dummyCarModified, dummyTruckModified);
	}
	
	@Test
	void build_joinedTables_oneToManyWithMappedAssociation_eachSubclassRedeclaresSameAssociation_throwsException() {
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleMappingBuilder = entityBuilder(Vehicle.class, LONG_TYPE)
				.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
				.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
						.addSubClass(subentityBuilder(Car.class)
								.map(Car::getModel)
								.mapOneToMany(Car::getWheels, entityBuilder(Wheel.class, String.class)
										// please note that we use an already-assigned policy because it requires entities to be marked
										// as persisted after select, so we test also select listener of relation
										.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
										.map(Wheel::getModel)).mappedBy(Wheel::setVehicle))
						.addSubClass(subentityBuilder(Truck.class)
								.map(Truck::getColor)
								.mapOneToMany(Truck::getWheels, entityBuilder(Wheel.class, String.class)
										// please note that we use an already-assigned policy because it requires entities to be marked
										// as persisted after select, so we test also select listener of relation
										.mapKey(Wheel::getSerialNumber, alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
										.map(Wheel::getModel)).mappedBy(Wheel::setVehicle))
				);
		
		PersistenceContext persistenceContext5 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		assertThatCode(() -> vehicleMappingBuilder.build(persistenceContext5))
				// Note that Exception type is not so important nor message actually : this test shows that this
				// feature is not supported, but it's difficult to check in code therefore we don't throw an UnsupportedOperationException
				.hasMessage("A foreign key with same source columns but different referenced columns already exist :"
						+ " 'FK_Wheel_vehicleId_Car_id' <Wheel.vehicleId -> Car.id>"
						+ " vs wanted new one"
						+ " 'FK_Wheel_vehicleId_Truck_id' <Wheel.vehicleId -> Truck.id>");
	}
	
	static Object[][] crud_polymorphism_subClassHasElementCollection() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		Object[][] result = new Object[][] {
				{	"single table",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel)
										.mapCollection(Car::getPlates, String.class), "CAR")
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor), "TRUCK"))
						.build(persistenceContext1),
						persistenceContext1,
						PolymorphismType.SINGLE_TABLE },
				{	"joined tables",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel)
										.mapCollection(Car::getPlates, String.class))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor)))
						.build(persistenceContext2),
						persistenceContext2,
						PolymorphismType.JOIN_TABLE},
				{	"table per class",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getModel)
										.mapCollection(Car::getPlates, String.class))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getColor)))
						.build(persistenceContext3),
						persistenceContext3,
						PolymorphismType.TABLE_PER_CLASS},
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource
	void crud_polymorphism_subClassHasElementCollection(String testDisplayName,
														EntityPersister<AbstractVehicle, Identifier<Long>> persister,
														PersistenceContext persistenceContext,
														PolymorphismType polymorphismType) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.addPlate("XYZ-ABC-01");
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
		dummyCarModified.addPlate("XYZ-ABC-02");
		Truck dummyTruckModified = new Truck(2L);
		dummyTruckModified.setColor(new Color(99));
		
		persister.update(dummyCarModified, dummyCar, true);
		
		persister.update(dummyTruckModified, dummyTruck, true);
		
		// committing before deletion because we'll rollback after it to resume state with data
		persistenceContext.getConnectionProvider().giveConnection().commit();
		persister.delete(Arrays.asList(dummyCarModified, dummyTruckModified));
		// nothing to delete because all was deleted by cascade
		String sql = null;
		switch (polymorphismType) {
			case SINGLE_TABLE:
				sql = "select count(*) as cnt from Car_plates";
				break;
			case JOIN_TABLE:
				sql = "select count(*) as cnt from Car_plates";
				break;
			case TABLE_PER_CLASS:
				sql = "select count(*) as cnt from Car_plates";
				break;
			default:
				throw new IllegalArgumentException();
		}
		Integer plateCount = Nullable.nullable(sql)
				.map(query -> persistenceContext.newQuery(query, int.class)
						.mapKey("cnt", int.class)
						.execute(Accumulators.getFirst()))
				.getOr(0);
		assertThat(plateCount).isEqualTo(0);
		persistenceContext.getConnectionProvider().giveConnection().rollback();
		
		assertThat(persister.select(dummyTruck.getId())).isEqualTo(dummyTruckModified);
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		assertThat(selectedCar).isEqualTo(dummyCarModified);
		assertThat(((Car) selectedCar).getPlates()).containsExactlyInAnyOrder("XYZ-ABC-02");
		assertThat(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruck.getId()))).containsExactlyInAnyOrder(dummyCarModified, dummyTruckModified);
	}
	
	@Nested
	class OneToSingleTableOne {
		
		@Test
		void oneSubClass() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
										.map(Car::getId)
										.map(Car::getModel)
										.map(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from Vehicle", String.class)
					.mapKey("model", String.class);
			
			Set<String> allCars = modelQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			Set<String> existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor), "CAR")
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getColor), "TRUCK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Duo<String, Integer>> modelQuery = persistenceContext.newQuery("select * from Vehicle", (Class<Duo<String, Integer>>) (Class) Duo.class)
					.mapKey(Duo::new, "model", String.class, "color", Integer.class);
			
			Set<Duo<String, Integer>> allCars = modelQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactlyInAnyOrder(new Duo<>("Renault", 666), new Duo<>(null, 42));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			Set<Duo<String, Integer>> existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactlyInAnyOrder(new Duo<>("Peugeot", 666), new Duo<>(null, 42));
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			Set<Vehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruck);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactlyInAnyOrder(new Duo<>(null, 42));
			
			abstractVehiclePersister.delete(dummyTruck);
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel), "CAR")
							.addSubClass(subentityBuilder(Truck.class),
									"TRUCK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='CAR'", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='TRUCK'", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery1 = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42)));
			Set<Vehicle> loadedVehicles = vehicleExecutableQuery1.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256)));
			loadedVehicles = vehicleExecutableQuery.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from Vehicle where id = " + dummyTruck.getId().getSurrogate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			PersistListener persistListenerMock = mock(PersistListener.class);
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addPersistListener(persistListenerMock);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			abstractVehiclePersister.update(dummyCar, dummyCar, true);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
			
			// persist test
			// We need to cleanup previous mocks interactions because persist(..) will trigger them again, this avoids "times(2)" in verify(..)
			clearInvocations(insertListenerMock, updateListenerMock, selectListenerMock);
			// Recreating a dummy Car since previous one is deleted and we can't simulate a new instance through car.getId().markNotPersisted()
			// because it doesn't exist and we don't want to create it for this particular use case.
			dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			abstractVehiclePersister.persist(dummyCar);
			verify(persistListenerMock).beforePersist(Arrays.asHashSet(dummyCar));
			verify(persistListenerMock).afterPersist(Arrays.asHashSet(dummyCar));
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			verify(selectListenerMock, times(2)).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			// select test
			clearInvocations(selectListenerMock);
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asHashSet(loadedCar));
		}
	}
	
	@Nested
	class OneToJoinedTablesOne {
		
		@Test
		void oneSubClass() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Car", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from Vehicle left outer join car on Vehicle.id = car.id", String.class)
					.mapKey("model", String.class);
			
			Set<String> allCars = modelQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			Set<String> existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId)
									.map(Truck::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Car", "Truck", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from Vehicle", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> vehicleIds = vehicleIdQuery.execute(Accumulators.toSet());
			assertThat(vehicleIds).containsExactly(1, 2);
			
			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			Set<Vehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruck);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleQuery = persistenceContext.newQuery("select"
					+ " count(*) as vehicleCount from Vehicle where id in ("
					+ dummyCar.getId().getSurrogate() + ", " + + dummyTruck.getId().getSurrogate() + ")", Integer.class)
					.mapKey("vehicleCount", Integer.class);
			
			Integer vehicleCount = Iterables.first(vehicleQuery.execute(Accumulators.toSet()));
			assertThat(vehicleCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from car where id = " + dummyTruck.getId().getSurrogate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel))
							.addSubClass(subentityBuilder(Truck.class)
									))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Car", "Truck", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from Vehicle", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> vehicleIds = vehicleIdQuery.execute(Accumulators.toSet());
			assertThat(vehicleIds).containsExactly(1, 2);
			
			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery1 = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42)));
			Set<Vehicle> loadedVehicles = vehicleExecutableQuery1.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256)));
			loadedVehicles = vehicleExecutableQuery.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from Vehicle where id = " + dummyTruck.getId().getSurrogate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			PersistListener persistListenerMock = mock(PersistListener.class);
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addPersistListener(persistListenerMock);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			abstractVehiclePersister.update(dummyCar, dummyCar, true);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
			
			// persist test
			// We need to cleanup previous mocks interactions because persist(..) will trigger them again, this avoids "times(2)" in verify(..)
			clearInvocations(insertListenerMock, updateListenerMock, selectListenerMock);
			// Recreating a dummy Car since previous one is deleted and we can't simulate a new instance through car.getId().markNotPersisted()
			// because it doesn't exist and we don't want to create it for this particular use case.
			dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			abstractVehiclePersister.persist(dummyCar);
			verify(persistListenerMock).beforePersist(Arrays.asHashSet(dummyCar));
			verify(persistListenerMock).afterPersist(Arrays.asHashSet(dummyCar));
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			verify(selectListenerMock, times(2)).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			// select test
			clearInvocations(selectListenerMock);
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asHashSet(loadedCar));
		}
	}
	
	@Nested
	class OneToTablePerClassOne {
		
		@Test
		void oneSubClass() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from car", String.class)
					.mapKey("model", String.class);
			
			Set<String> allCars = modelQuery.execute(Accumulators.toSet());
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			Set<String> existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getColor)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truck", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));

			Truck dummyTruck = new Truck(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruck.setColor(new Color(42));

			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));

			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);

			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			Set<Vehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruck);

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));

			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);

			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from car where id = " + dummyTruck.getId().getSurrogate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel))
							.addSubClass(subentityBuilder(Truck.class)
									))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truck", "Engine");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));

			Truck dummyTruck = new Truck(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruck.setColor(new Color(42));

			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));

			ExecutableBeanPropertyQueryMapper<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> carIds = carIdQuery.execute(Accumulators.toSet());
			assertThat(carIds).containsExactly(1);

			ExecutableBeanPropertyQueryMapper<Integer> truckIdQuery = persistenceContext.newQuery("select id from truck", Integer.class)
					.mapKey("id", Integer.class);
			
			Set<Integer> truckIds = truckIdQuery.execute(Accumulators.toSet());
			assertThat(truckIds).containsExactly(2);

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery1 = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42)));
			Set<Vehicle> loadedVehicles = vehicleExecutableQuery1.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruck);
			
			ExecutableQuery<Vehicle> vehicleExecutableQuery = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(666)));
			loadedVehicles = vehicleExecutableQuery.execute(Accumulators.toSet());
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));

			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);

			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from truck where id = " + dummyTruck.getId().getSurrogate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableBeanPropertyQueryMapper<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey("id", Long.class);
			
			assertThat(engineQuery.execute(Accumulators.toSet())).isEmpty();
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.mapKey(Engine::getId, ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			PersistListener persistListenerMock = mock(PersistListener.class);
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addPersistListener(persistListenerMock);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			abstractVehiclePersister.update(dummyCar, dummyCar, true);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
			
			// persist test
			// We need to cleanup previous mocks interactions because persist(..) will trigger them again, this avoids "times(2)" in verify(..)
			clearInvocations(insertListenerMock, updateListenerMock, selectListenerMock);
			// Recreating a dummy Car since previous one is deleted and we can't simulate a new instance through car.getId().markNotPersisted()
			// because it doesn't exist and we don't want to create it for this particular use case.
			dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			abstractVehiclePersister.persist(dummyCar);
			verify(persistListenerMock).beforePersist(Arrays.asHashSet(dummyCar));
			verify(persistListenerMock).afterPersist(Arrays.asHashSet(dummyCar));
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			verify(selectListenerMock, times(2)).beforeSelect(Arrays.asHashSet(dummyCar.getId()));
			
			// select test
			clearInvocations(selectListenerMock);
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asHashSet(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asHashSet(loadedCar));
		}
	}
	
	@Nested
	class OneToPolymorphicOne {
		
		@Test
		void oneToJoinedTable_crud() {
			FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
									.addSubClass(subentityBuilder(Truck.class))
									.addSubClass(subentityBuilder(Car.class))
							);
			EntityPersister<Vehicle, Identifier<Long>> vehiclePersister = vehicleConfiguration.build(persistenceContext);
			
			EntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Person person = new Person(1);
			person.setVehicle(new Car(42L));
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating one-to-one relation
			person.setVehicle(new Truck(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(42L))).isNull();
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(666L))).isNull();
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truck(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// testing deletion
			testInstance.delete(person);
			assertThat(testInstance.select(person.getId())).isNull();
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(17L))).isNull();
		}
		
		@Test
		void oneToJoinedTable_crud_ownedByReverseSide() {
			FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
									.addSubClass(subentityBuilder(Truck.class))
									.addSubClass(subentityBuilder(Car.class))
							);
			
			EntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Person::getVehicle, vehicleConfiguration).mappedBy(Vehicle::getOwner).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we create a dedicated and simple Vehicle persister to help us select Vehicle entity, it won't be the same as the one under Person
			EntityPersister<Vehicle, Identifier<Long>> vehiclePersister = vehicleConfiguration.build(persistenceContext);
			
			// insert
			Person person = new Person(1);
			person.setVehicle(new Car(42L));
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating one-to-one relation
			person.setVehicle(new Truck(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(42L))).isNull();
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(666L))).isNull();
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truck(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// testing deletion
			testInstance.delete(person);
			assertThat(testInstance.select(person.getId())).isNull();
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(17L))).isNull();
		}
		
		@Test
		void oneToSingleTable_crud() {
			FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.map(Vehicle::getColor)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
									.addSubClass(subentityBuilder(Truck.class), "T")
									.addSubClass(subentityBuilder(Car.class), "C")
							);
			EntityPersister<Vehicle, Identifier<Long>> vehiclePersister = vehicleConfiguration.build(persistenceContext);
			
			EntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Person person = new Person(1);
			Car car = new Car(42L);
			car.setColor(new Color(17));
			person.setVehicle(car);
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficient for ou checking
			// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
			ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
					.addProperty(Vehicle::getId)
					.addProperty(Vehicle::getClass)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.build();
			ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
					.addProperty(Person::getId)
					.addProperty(Person::getName)
					.addProperty(Person::getTimestamp)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.withPrinter(Vehicle.class, vehiclePrinter::toString)
					.build();
			
			assertThat(loadedPerson)
					.usingComparator(Comparator.comparing(personPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
					.isEqualTo(person);
			// ensuring that reverse side is also set
			assertThat(loadedPerson.getVehicle().getOwner()).isEqualTo(loadedPerson);
			
			// updating one-to-one relation
			person.setVehicle(new Truck(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson)
					.usingComparator(Comparator.comparing(personPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
					.isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(42L))).isNull();
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(666L))).isNull();
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truck(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// testing deletion
			testInstance.delete(person);
			assertThat(testInstance.select(person.getId())).isNull();
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(17L))).isNull();
		}
		
		@Test
		void oneToSingleTable_crud_ownedByReverseSide() {
			FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.map(Vehicle::getColor)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
									.addSubClass(subentityBuilder(Truck.class), "T")
									.addSubClass(subentityBuilder(Car.class), "C")
							);
			
			EntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.mapKey(Person::getId, ALREADY_ASSIGNED)
					.mapOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL).mappedBy(Vehicle::getOwner)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// we create a dedicated and simple Vehicle persister to help us select Vehicle entity, it won't be the same as the one under Person
			EntityPersister<Vehicle, Identifier<Long>> vehiclePersister = vehicleConfiguration.build(persistenceContext);
			
			// insert
			Person person = new Person(1);
			Car car = new Car(42L);
			car.setColor(new Color(17));
			person.setVehicle(car);
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficient for ou checking
			// Note that we don't use ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursion 
			ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
					.addProperty(Vehicle::getId)
					.addProperty(Vehicle::getClass)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.build();
			ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
					.addProperty(Person::getId)
					.addProperty(Person::getName)
					.addProperty(Person::getTimestamp)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.withPrinter(Vehicle.class, vehiclePrinter::toString)
					.build();
			
			assertThat(loadedPerson)
					.usingComparator(Comparator.comparing(personPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
					.isEqualTo(person);
			// ensuring that reverse side is also set
			assertThat(loadedPerson.getVehicle().getOwner()).isEqualTo(loadedPerson);
			
			// updating one-to-one relation
			person.setVehicle(new Truck(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson)
					.usingComparator(Comparator.comparing(personPrinter::toString))
					.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
					.isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(42L))).isNull();
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(666L))).isNull();
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truck(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertThat(loadedPerson).isEqualTo(person);
			
			// testing deletion
			testInstance.delete(person);
			assertThat(testInstance.select(person.getId())).isNull();
			// checking for orphan removal (relation was marked as such)
			assertThat(vehiclePersister.select(new PersistedIdentifier<>(17L))).isNull();
		}
		
	}
	
	static EntityPersister<Person, Identifier<Long>> generateOneToPolymorphicOneTestCase(PolymorphismType type,
																						 boolean ownedByReverseSide,
																						 boolean fetchSeparately,
																						 PersistenceContext persistenceContext) {
		FluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
				embeddableBuilder(Person.class)
						.map(Person::getName)
						.embed(Person::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		
		FluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
				entityBuilder(Vehicle.class, LONG_TYPE)
						.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
						.map(Vehicle::getColor)
						.mapPolymorphism(giveVehiclePolymorphismPolicy(type)
						);
		
		FluentMappingBuilderOneToOneOptions<Person, Identifier<Long>, ?, Vehicle> persisterConfiguration = entityBuilder(Person.class, LONG_TYPE)
				.mapKey(Person::getId, ALREADY_ASSIGNED)
				.mapOneToOne(Person::getVehicle, vehicleConfiguration)
				.cascading(RelationMode.ALL_ORPHAN_REMOVAL);
		
		persisterConfiguration.mapSuperClass(timestampedPersistentBeanMapping);
		
		if (ownedByReverseSide) {
			persisterConfiguration.mappedBy(Vehicle::getOwner);
		}
		
		if (fetchSeparately) {
			persisterConfiguration.fetchSeparately();
		}
		
		EntityPersister<Person, Identifier<Long>> result = persisterConfiguration.build(persistenceContext);
		
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// we create a dedicated and simple Vehicle persister to help us select Vehicle entity, it won't be the same as the one under Person
		vehicleConfiguration.build(persistenceContext);
		
		return result;
	}
	
	static PolymorphismPolicy<Vehicle> giveVehiclePolymorphismPolicy(PolymorphismType type) {
		switch (type) {
			case SINGLE_TABLE:
				return PolymorphismPolicy.<Vehicle>singleTable()
						.addSubClass(subentityBuilder(Truck.class), "T")
						.addSubClass(subentityBuilder(Car.class), "C");
			case JOIN_TABLE:
				return PolymorphismPolicy.<Vehicle>joinTable()
						.addSubClass(subentityBuilder(Truck.class))
						.addSubClass(subentityBuilder(Car.class));
			case TABLE_PER_CLASS:
				return PolymorphismPolicy.<Vehicle>tablePerClass()
						.addSubClass(subentityBuilder(Truck.class))
						.addSubClass(subentityBuilder(Car.class));
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	public static Stream<Object[]> oneToPolymorphicOne_crud() {
		// Building result by making a combination of 3 infos :
		// - polymorphism type,
		// - with association table,
		// - load relation separately
		return Stream.of(PolymorphismType.values())
				.flatMap(type -> Stream.of(true, false)
						.flatMap(withAssociationTable -> Stream.of(true, false)
								// we skip TABLE_PER_CLASS with relation owned by reverse side because it can't be implemented
								.filter(x -> !(type == PolymorphismType.TABLE_PER_CLASS && !withAssociationTable))
								.map(fetchSeparately -> {
									PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
									String testCaseName = new StringAppender().ccat(withAssociationTable ? "association table" : "relation owned by table", fetchSeparately ? "fetch separately" : "query with join", type, ", ").toString();
									return new Object[] {
											testCaseName,
											generateOneToPolymorphicOneTestCase(type, withAssociationTable, fetchSeparately, persistenceContext), persistenceContext,
											// following info are only for assertion purpose
											withAssociationTable,
											type == PolymorphismType.TABLE_PER_CLASS };
								})));
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	void oneToPolymorphicOne_crud(String displayName, EntityPersister<Person, Identifier<Long>> testInstance, PersistenceContext persistenceContext,
								   boolean withAssociationTable,
								   boolean isTablePerClass) {
		// insert
		Person person = new Person(1);
		Car car = new Car(42L);
		car.setColor(new Color(17));
		person.setVehicle(car);
		testInstance.insert(person);
		Person loadedPerson = testInstance.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		
		// updating embedded value
		person.setTimestamp(new Timestamp());
		testInstance.update(person, loadedPerson, true);
		
		loadedPerson = testInstance.select(person.getId());
		
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for ou checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<Vehicle> vehiclePrinter = new ObjectPrinterBuilder<Vehicle>()
				.addProperty(Vehicle::getId)
				.addProperty(Vehicle::getClass)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
				.build();
		ObjectPrinter<Person> personPrinter = new ObjectPrinterBuilder<Person>()
				.addProperty(Person::getId)
				.addProperty(Person::getName)
				.addProperty(Person::getTimestamp)
				.addProperty(Person::getVehicle)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
				.withPrinter(Vehicle.class, vehiclePrinter::toString)
				.build();
		
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
		// ensuring that reverse side is also set
		assertThat(loadedPerson.getVehicle().getOwner()).isEqualTo(loadedPerson);
		
		// updating one-to-one relation
		person.setVehicle(new Truck(666L));
		testInstance.update(person, loadedPerson, true);
		
		loadedPerson = testInstance.select(person.getId());
		assertThat(loadedPerson)
				.usingComparator(Comparator.comparing(personPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(Person.class, personPrinter))
				.isEqualTo(person);
		// checking for orphan removal (relation was marked as such)
		assertThat(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(42L))).isNull();
		
		// nullifying one-to-one relation
		person.setVehicle(null);
		testInstance.update(person, loadedPerson, true);
		
		loadedPerson = testInstance.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		// checking for orphan removal (relation was marked as such)
		assertThat(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(666L))).isNull();
		
		
		// setting new one-to-one relation
		person.setVehicle(new Truck(17L));
		testInstance.update(person, loadedPerson, true);
		
		loadedPerson = testInstance.select(person.getId());
		assertThat(loadedPerson).isEqualTo(person);
		
		// testing deletion
		testInstance.delete(person);
		assertThat(testInstance.select(person.getId())).isNull();
		// checking for orphan removal (relation was marked as such)
		assertThat(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(17L))).isNull();
	}
	
	
	private enum PolymorphismType {
		SINGLE_TABLE,
		JOIN_TABLE,
		TABLE_PER_CLASS
	}
	
	static EntityPersister<Country, Identifier<Long>> generateOneToManyTestCase(PolymorphismType type,
																				boolean withAssociationTable,
																				boolean fetchSeparately,
																				PersistenceContext persistenceContext) {
		FluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
				embeddableBuilder(Country.class)
						.map(Country::getName)
						.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate)
								.map(Timestamp::getModificationDate));
		
		FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
				entityBuilder(City.class, LONG_TYPE)
						.mapKey(City::getId, ALREADY_ASSIGNED)
						.map(City::getName)
						.mapPolymorphism(givePolymorphismPolicy(type));
		
		FluentMappingBuilderOneToManyOptions<Country, Identifier<Long>, City, Set<City>> persisterConfiguration = entityBuilder(Country.class, LONG_TYPE)
				.mapKey(Country::getId, ALREADY_ASSIGNED)
				.mapOneToMany(Country::getCities, cityConfiguration)
				.cascading(RelationMode.ALL);
		
		persisterConfiguration
				.mapSuperClass(timestampedPersistentBeanMapping);
		
		if (withAssociationTable) {
			persisterConfiguration.reverselySetBy(City::setCountry);    // necessary if you want bidirectionnality to be set in memory
		} else {
			persisterConfiguration.mappedBy(City::getCountry);
		}
		
		if (fetchSeparately) {
			persisterConfiguration.fetchSeparately();
		}
		
		return persisterConfiguration.build(persistenceContext);
	}
	
	static PolymorphismPolicy<City> givePolymorphismPolicy(PolymorphismType type) {
		switch (type) {
			case SINGLE_TABLE:
				return PolymorphismPolicy.<City>singleTable()
						.addSubClass(subentityBuilder(Village.class)
								.map(Village::getBarCount), "V")
						.addSubClass(subentityBuilder(Town.class)
								.map(Town::getDiscotecCount), "T");
			case JOIN_TABLE:
				return PolymorphismPolicy.<City>joinTable()
						.addSubClass(subentityBuilder(Village.class)
								.map(Village::getBarCount))
						.addSubClass(subentityBuilder(Town.class)
								.map(Town::getDiscotecCount));
			case TABLE_PER_CLASS:
				return PolymorphismPolicy.<City>tablePerClass()
						.addSubClass(subentityBuilder(Village.class)
								.map(Village::getBarCount))
						.addSubClass(subentityBuilder(Town.class)
								.map(Town::getDiscotecCount));
			default:
				throw new UnsupportedOperationException();
		}
	}
	
	public static Stream<Object[]> oneToPolymorphicMany_crud() {
		// Building result by making a combination of 3 infos :
		// - polymorphism type,
		// - with association table,
		// - load relation separately
		return Stream.of(PolymorphismType.values())
				.flatMap(type -> Stream.of(true, false)
						.flatMap(withAssociationTable -> Stream.of(true, false)
								.map(fetchSeparately -> {
									PersistenceContext persistenceContext = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
									String testCaseName = new StringAppender().ccat(withAssociationTable ? "association table" : "relation owned by table", fetchSeparately ? "fetch separately" : "query with join", type, ", ").toString();
									return new Object[] {
											testCaseName,
											generateOneToManyTestCase(type, withAssociationTable, fetchSeparately, persistenceContext), persistenceContext,
											// following info are only for assertion purpose
											withAssociationTable,
											type == PolymorphismType.TABLE_PER_CLASS };
								})));
	}
	
	@ParameterizedTest(name = "{0}")
	@MethodSource
	void oneToPolymorphicMany_crud(String displayName, EntityPersister<Country, Identifier<Long>> testInstance, PersistenceContext persistenceContext,
								   boolean withAssociationTable,
								   boolean isTablePerClass) {
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		// insert
		Country country = new Country(1L);
		Village grenoble = new Village(42L);
		grenoble.setName("Grenoble");
		country.addCity(grenoble);
		testInstance.insert(country);
		
		// testing select
		Country loadedCountry = testInstance.select(country.getId());
		assertThat(loadedCountry).isEqualTo(country);
		
		// we use a printer to compare our results because entities override equals() which only keep "id" into account
		// which is far from sufficient for our checking
		// Note that we don't use ObjectPrinterBuilder#printerFor because it takes getCities() into account whereas its code is not ready for recursion 
		ObjectPrinter<Country> countryPrinter = new ObjectPrinterBuilder<Country>()
				.addProperty(Country::getId)
				.addProperty(Country::getName)
				.addProperty(Country::getTimestamp)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
				.build();
		ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
				.addProperty(City::getId)
				.addProperty(Village::getBarCount)
				.addProperty(Town::getDiscotecCount)
				.addProperty(City::getName)
				.addProperty(City::getCountry)
				.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
				.withPrinter(Country.class, countryPrinter::toString)
				.build();
		
		assertThat(loadedCountry.getCities())
				.usingElementComparator(Comparator.comparing(cityPrinter::toString))
				.withRepresentation(new PartialRepresentation<>(City.class, cityPrinter))
				.isEqualTo(country.getCities());
		
		// ensuring that reverse side is also set
		assertThat(loadedCountry.getCities())
				.extracting(City::getCountry)
				.containsExactlyInAnyOrder(loadedCountry);
		
		// testing update and select of mixed type of City
		Town lyon = new Town(17L);
		lyon.setDiscotecCount(123);
		lyon.setName("Lyon");
		country.addCity(lyon);
		grenoble.setBarCount(51);
		testInstance.update(country, loadedCountry, true);
		
		loadedCountry = testInstance.select(country.getId());
		// resulting select must contain Town and Village
		assertThat(loadedCountry.getCities()).containsExactlyInAnyOrder(grenoble, lyon);
		// bidirectionality must be preserved
		assertThat(loadedCountry.getCities()).extracting(City::getCountry).containsExactlyInAnyOrder(loadedCountry, loadedCountry);
		
		// testing update : removal of a city, reversed column must be set to null
		Country modifiedCountry = new Country(country.getId());
		modifiedCountry.addCity(Iterables.first(country.getCities()));
		
		testInstance.update(modifiedCountry, country, false);
		// there's only 1 relation in table
		Set<Long> cityCountryIds;
		if (withAssociationTable) {
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select Country_Id from Country_cities", Long.class)
					.mapKey("Country_Id", Long.class);
			cityCountryIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(cityCountryIds)).containsExactlyInAnyOrder(country.getId().getSurrogate());
		} else if (isTablePerClass) {
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select CountryId from Town union all select CountryId from Village", Long.class)
					.mapKey("CountryId", Long.class);
			cityCountryIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(cityCountryIds)).containsExactlyInAnyOrder(country.getId().getSurrogate(), null);
		} else {
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select CountryId from City", Long.class)
					.mapKey("CountryId", Long.class);
			cityCountryIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(new HashSet<>(cityCountryIds)).containsExactlyInAnyOrder(country.getId().getSurrogate(), null);
		}
		
		// testing delete
		testInstance.delete(modifiedCountry);
		// Cities shouldn't be deleted (we didn't ask for delete orphan)
		Set<City> select;
		String sql = isTablePerClass
				? "select * from Town union select * from Village"
				: "select * from City";
		select = persistenceContext.newQuery(sql, City.class)
				.mapKey(City::new, "id", long.class, "name", String.class)
				.execute(Accumulators.toSet());
		assertThat(select).hasSize(2);
	}
	
	@Nested
	class OneToPolymorphicMany {
		
		@Test
		void oneToTablePerClass_crud_ownedByReverseSide_foreignKeysAreCreated() throws SQLException {
			
			FluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.map(Country::getName)
							.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City>tablePerClass()
									.addSubClass(subentityBuilder(Village.class)
											.map(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.map(Town::getDiscotecCount))
							);
			
			EntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Country::getCities, cityConfiguration)
					.mappedBy(City::getCountry)
					.cascading(RelationMode.ALL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().giveConnection();
			ResultSetIterator<JdbcForeignKey> fkVillageIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData()
					.getImportedKeys(null, null, "VILLAGE")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			JdbcForeignKey foundForeignKey = Iterables.first(fkVillageIterator);
			JdbcForeignKey expectedForeignKey = new JdbcForeignKey("FK_VILLAGE_COUNTRYID_COUNTRY_ID", "VILLAGE", "COUNTRYID", "COUNTRY", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
			
			ResultSetIterator<JdbcForeignKey> fkTownIterator = new ResultSetIterator<JdbcForeignKey>(currentConnection.getMetaData()
					.getImportedKeys(null, null, "TOWN")) {
				@Override
				public JdbcForeignKey convert(ResultSet rs) throws SQLException {
					return new JdbcForeignKey(
							rs.getString("FK_NAME"),
							rs.getString("FKTABLE_NAME"), rs.getString("FKCOLUMN_NAME"),
							rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")
					);
				}
			};
			foundForeignKey = Iterables.first(fkTownIterator);
			expectedForeignKey = new JdbcForeignKey("FK_TOWN_COUNTRYID_COUNTRY_ID", "TOWN", "COUNTRYID", "COUNTRY", "ID");
			assertThat(foundForeignKey.getSignature()).isEqualTo(expectedForeignKey.getSignature());
		}
			
		@Test
		void oneToTablePerClass_crud_ownedByReverseSide() {
			
			FluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.map(Country::getName)
							.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate));
			
			FluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.mapKey(City::getId, ALREADY_ASSIGNED)
							.map(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City>tablePerClass()
									.addSubClass(subentityBuilder(Village.class)
											.map(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.map(Town::getDiscotecCount))
							);
			
			EntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.mapKey(Country::getId, ALREADY_ASSIGNED)
					.mapOneToMany(Country::getCities, cityConfiguration)
					.mappedBy(City::getCountry)
					.cascading(RelationMode.ALL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Country country = new Country(1L);
			Village grenoble = new Village(42L);
			grenoble.setName("Grenoble");
			country.addCity(grenoble);
			testInstance.insert(country);
			
			// testing select
			Country loadedCountry = testInstance.select(country.getId());
			assertThat(loadedCountry).isEqualTo(country);
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficient for ou checking
			// Note that we don't use ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursion 
			ObjectPrinter<Country> countryPrinter = new ObjectPrinterBuilder<Country>()
					.addProperty(Country::getId)
					.addProperty(Country::getName)
					.addProperty(Country::getTimestamp)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.build();
			ObjectPrinter<City> cityPrinter = new ObjectPrinterBuilder<City>()
					.addProperty(City::getId)
					.addProperty(Village::getBarCount)
					.addProperty(Town::getDiscotecCount)
					.addProperty(City::getName)
					.addProperty(City::getCountry)
					.withPrinter(AbstractIdentifier.class, Functions.chain(AbstractIdentifier::getSurrogate, String::valueOf))
					.withPrinter(Country.class, countryPrinter::toString)
					.build();
			
			assertThat(loadedCountry.getCities())
					.usingElementComparator(Comparator.comparing(cityPrinter::toString))
					.isEqualTo(country.getCities());
			// ensuring that reverse side is also set
			assertThat(loadedCountry.getCities()).extracting(City::getCountry).containsExactlyInAnyOrder(loadedCountry);
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertThat(loadedCountry.getCities()).containsExactlyInAnyOrder(grenoble, lyon);
			// bidirectionality must be preserved
			assertThat(loadedCountry.getCities()).extracting(City::getCountry).containsExactlyInAnyOrder(loadedCountry, loadedCountry);
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId().getSurrogate());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			ExecutableQuery<Long> longExecutableQuery1 = persistenceContext.newQuery("select countryId from Town union all select countryId from Village", Long.class)
					.mapKey(i -> i, "countryId", Long.class);
			Set<Long> cityCountryIds = longExecutableQuery1.execute(Accumulators.toSet());
			assertThat(cityCountryIds).containsExactlyInAnyOrder(country.getId().getSurrogate(), null);
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			ExecutableQuery<Long> longExecutableQuery = persistenceContext.newQuery("select id from Town union all select id from Village", Long.class)
					.mapKey(i -> i, "id", Long.class);
			Set<Long> cityIds = longExecutableQuery.execute(Accumulators.toSet());
			assertThat(cityIds).containsExactlyInAnyOrder(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate());
		}
	}
	
	private static class Republic extends Country {
		
		private Person primeMinister;
		
		private int deputeCount;
		
		public Republic() {
		}
		
		public Republic(Identifier<Long> id) {
			super(id);
		}
		
		public Person getPrimeMinister() {
			return primeMinister;
		}
		
		public void setPrimeMinister(Person primeMinister) {
			this.primeMinister = primeMinister;
		}
		
		public int getDeputeCount() {
			return deputeCount;
		}
		
		public void setDeputeCount(int deputeCount) {
			this.deputeCount = deputeCount;
		}
	}
}
