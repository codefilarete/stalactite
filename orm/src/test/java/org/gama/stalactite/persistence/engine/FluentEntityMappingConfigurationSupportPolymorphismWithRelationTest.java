package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.gama.stalactite.persistence.engine.IFluentEntityMappingBuilder.IFluentMappingBuilderPropertyOptions;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableSelect;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Car;
import org.gama.stalactite.persistence.engine.model.Car.Radio;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Engine;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.engine.model.Town;
import org.gama.stalactite.persistence.engine.model.Truk;
import org.gama.stalactite.persistence.engine.model.Vehicle;
import org.gama.stalactite.persistence.engine.model.Vehicle.Wheel;
import org.gama.stalactite.persistence.engine.model.Village;
import org.gama.stalactite.persistence.id.AbstractIdentifier;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.id.provider.LongProvider;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.result.ResultSetIterator;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.gama.trace.ObjectPrinterBuilder;
import org.gama.trace.ObjectPrinterBuilder.ObjectPrinter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.lang.test.Assertions.assertAllEquals;
import static org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy.alreadyAssigned;
import static org.gama.stalactite.persistence.engine.MappingEase.embeddableBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.subentityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportPolymorphismWithRelationTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
	}
	
	@BeforeEach
	public void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
	}
	
	
	static Object[][] polymorphicOneToOne_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		Object[][] result = new Object[][] {
				{	"single table",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
						.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
								.add(Engine::getId).identifier(ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getModel), "CAR")
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getColor), "TRUK"))
						.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"joined tables",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
						.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
								.add(Engine::getId).identifier(ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getModel))
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getColor)))
						.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"table per class",
					entityBuilder(Vehicle.class, LONG_TYPE)
						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
							.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
									.add(Engine::getId).identifier(ALREADY_ASSIGNED))
						.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>tablePerClass()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getModel))
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getColor)))
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
	void crudPolymorphicOneToOne(String testDisplayName, IEntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.setEngine(new Engine(100L));
		Truk dummyTruk = new Truk(2L);
		dummyTruk.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruk));
		
		Car dummyCarModfied = new Car(1L);
		dummyCarModfied.setModel("Peugeot");
		dummyCarModfied.setEngine(new Engine(200L));
		Truk dummyTrukModfied = new Truk(2L);
		dummyTrukModfied.setColor(new Color(99));
		
		persister.update(dummyCarModfied, dummyCar, true);
		
		persister.update(dummyTrukModfied, dummyTruk, true);
		
		connectionProvider.getCurrentConnection().commit();
		assertEquals(1, persister.delete(dummyCarModfied));
		assertEquals(1, persister.delete(dummyTrukModfied));
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(2, persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied)));
		
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(dummyTrukModfied, persister.select(dummyTruk.getId()));
		assertEquals(dummyCarModfied, persister.select(dummyCar.getId()));
		assertEquals(Arrays.asSet(dummyCarModfied, dummyTrukModfied), new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId()))));
	}
	
	static Object[][] polymorphism_trunkHasOneToMany_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		
		IFluentMappingBuilderPropertyOptions<Person, Identifier<Long>> personMappingBuilder = MappingEase.entityBuilder(Person.class,
				Identifier.LONG_TYPE)
				.add(Person::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(Person::getName);
		
		IFluentMappingBuilderPropertyOptions<City, Identifier<Long>> cityMappingBuilder = MappingEase.entityBuilder(City.class,
				Identifier.LONG_TYPE)
				.add(City::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);
		
		Object[][] result = new Object[][] {
				{	"single table",
						entityBuilder(Country.class, Identifier.LONG_TYPE)
								.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.add(Country::getName)
								.add(Country::getDescription)
								.addOneToOne(Country::getPresident, personMappingBuilder)
								.addOneToManySet(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry)
								.mapPolymorphism(PolymorphismPolicy.<Country, Identifier<Long>>singleTable()
										.addSubClass(subentityBuilder(Republic.class)
												.add(Republic::getDeputeCount), "Republic"))
								.build(persistenceContext1)
				},
				{	"joined tables",
						entityBuilder(Country.class, Identifier.LONG_TYPE)
								.add(Country::getId).identifier(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
								.add(Country::getName)
								.add(Country::getDescription)
								.addOneToOne(Country::getPresident, personMappingBuilder)
								.addOneToManySet(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry)
								.mapPolymorphism(PolymorphismPolicy.<Country, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Republic.class)
												.add(Republic::getDeputeCount)))
								.build(persistenceContext2)
				},
				// Not implementable : one-to-many with mappedby targeting a table-per-class polymorphism is not implemented due to fk constraint, how to ? forget foreign key ?
//				{	"table per class",
//						entityBuilder(Country.class, Identifier.LONG_TYPE)
//								.add(Country::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
//								.add(Country::getName)
//								.add(Country::getDescription)
//								.addOneToOne(Country::getPresident, personMappingBuilder)
//								.addOneToManySet(Country::getCities, cityMappingBuilder).mappedBy(City::setCountry).cascading(RelationMode.READ_ONLY)
//								.mapPolymorphism(PolymorphismPolicy.<Republic, Identifier<Long>>tablePerClass()
//										.addSubClass(subentityBuilder(Republic.class)
//												.add(Republic::getDeputeCount)))
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
	void crud_polymorphism_trunkHasOneToMany(String testDisplayName, IEntityPersister<Country, Identifier<Long>> countryPersister) {
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
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("French president", persistedCountry.getPresident().getName());
		assertEquals("Paris", Iterables.first(persistedCountry.getCities()).getName());
		assertEquals(250, persistedCountry.getDeputeCount());
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		assertTrue(Iterables.first(persistedCountry.getCities()).getId().isPersisted());
		
		// testing update cascade
		persistedCountry.getPresident().setName("New french president");
		City grenoble = new City(cityIdentifierProvider.giveNewIdentifier());
		grenoble.setName("Grenoble");
		persistedCountry.addCity(grenoble);
		countryPersister.update(persistedCountry, dummyCountry, true);
		
		persistedCountry = (Republic) countryPersister.select(dummyCountry.getId());
		assertEquals(new PersistedIdentifier<>(0L), persistedCountry.getId());
		assertEquals("New french president", persistedCountry.getPresident().getName());
		assertEquals(Arrays.asHashSet("Grenoble", "Paris"), Iterables.collect(persistedCountry.getCities(), City::getName, HashSet::new));
		assertTrue(persistedCountry.getPresident().getId().isPersisted());
		assertTrue(Iterables.first(persistedCountry.getCities()).getId().isPersisted());
	}
	
	static Object[][] polymorphism_subClassHasOneToOne_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		Object[][] result = new Object[][] {
					{	"single table / one-to-one with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.add(Engine::getId).identifier(ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToOne(Car::getRadio, entityBuilder(Radio.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Radio::getSerialNumber).identifier(alreadyAssigned(Radio::markAsPersisted, Radio::isPersisted))
														.add(Radio::getModel)).mappedBy(Radio::getCar), "CAR")
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor), "TRUK"))
								.build(persistenceContext1),
							persistenceContext1.getConnectionProvider() },
				{	"joined tables / one-to-one with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToOne(Car::getRadio, entityBuilder(Radio.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Radio::getSerialNumber).identifier(alreadyAssigned(Radio::markAsPersisted, Radio::isPersisted))
														.add(Radio::getModel)).mappedBy(Radio::getCar))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)))
								.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
//				{	"table per class",
//					entityBuilder(Vehicle.class, LONG_TYPE)
//						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
//							.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
//									.add(Engine::getId).identifier(ALREADY_ASSIGNED))
//						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
//								.addSubClass(subentityBuilder(Car.class)
//										.add(Car::getModel))
//								.addSubClass(subentityBuilder(Truk.class)
//										.add(Truk::getColor)))
//						.build(persistenceContext3),
//						persistenceContext3.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_subClassHasOneToOne_data")
	void crud_polymorphism_subClassHasOneToOne(String testDisplayName, IEntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.setRadio(new Radio("XYZ-ABC-01"));
		Truk dummyTruk = new Truk(2L);
		dummyTruk.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruk));
		
		Car dummyCarModfied = new Car(1L);
		dummyCarModfied.setModel("Peugeot");
		dummyCarModfied.setRadio(new Radio("XYZ-ABC-02"));
		Truk dummyTrukModfied = new Truk(2L);
		dummyTrukModfied.setColor(new Color(99));
		
		persister.update(dummyCarModfied, dummyCar, true);
		
		persister.update(dummyTrukModfied, dummyTruk, true);
		
		connectionProvider.getCurrentConnection().commit();
		assertEquals(1, persister.delete(dummyCarModfied));
		assertEquals(1, persister.delete(dummyTrukModfied));
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(2, persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied)));
		
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(dummyTrukModfied, persister.select(dummyTruk.getId()));
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		assertEquals(dummyCarModfied, selectedCar);
		assertTrue(((Car) selectedCar).getRadio().isPersisted());	// testing afterSelect listener of sub entities relations 
		assertEquals(dummyCarModfied.getRadio(), ((Car) selectedCar).getRadio());
		assertEquals(Arrays.asSet(dummyCarModfied, dummyTrukModfied), new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId()))));
	}
	
	static Object[][] polymorphism_subClassHasOneToMany_data() {
		// each test has each own context so they can't pollute each other
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext4 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext5 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext6 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		Table wheelTable1 = new Table("Wheel");
		Column<Table, Integer> indexColumn1 = wheelTable1.addColumn("idx", Integer.class);
		Table wheelTable2 = new Table("Wheel");
		Column<Table, Integer> indexColumn2 = wheelTable2.addColumn("idx", Integer.class);
		Table wheelTable3 = new Table("Wheel");
		Column<Table, Integer> indexColumn3 = wheelTable3.addColumn("idx", Integer.class);
		Table wheelTable4 = new Table("Wheel");
		Column<Table, Integer> indexColumn4 = wheelTable4.addColumn("idx", Integer.class);
		Object[][] result = new Object[][] {
				{	"single table / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.add(Engine::getId).identifier(ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToManyList(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).reverselySetBy(Wheel::setVehicle), "CAR")
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor), "TRUK"))
								.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"single table / one-to-many with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
										.add(Engine::getId).identifier(ALREADY_ASSIGNED))
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToManyList(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).indexedBy(indexColumn1).mappedBy(Wheel::setVehicle), "CAR")
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor), "TRUK"))
								.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"joined tables / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToManyList(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).reverselySetBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)))
								.build(persistenceContext3),
						persistenceContext3.getConnectionProvider() },
				{	"joined tables / one-to-many with mapped association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToManyList(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).indexedBy(indexColumn2).mappedBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)))
								.build(persistenceContext4),
						persistenceContext4.getConnectionProvider() },
				{	"joined tables / one-to-many with mapped association / each subclass declares its association",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addOneToManyList(Car::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).indexedBy(indexColumn3).mappedBy(Wheel::setVehicle))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)
												.addOneToManyList(Truk::getWheels, entityBuilder(Wheel.class, String.class)
														// please note that we use an already-assigned policy because it requires entities to be mark
														// as persisted after select, so we test also select listener of relation
														.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
														.add(Wheel::getModel)).indexedBy(indexColumn3).mappedBy(Wheel::setVehicle))
								)
								.build(persistenceContext5),
						persistenceContext5.getConnectionProvider() },
				{	"joined tables / one-to-many with mapped association / association is defined as common property",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.addOneToManyList(Vehicle::getWheels, entityBuilder(Wheel.class, String.class)
										// please note that we use an already-assigned policy because it requires entities to be mark
										// as persisted after select, so we test also select listener of relation
										.add(Wheel::getSerialNumber).identifier(alreadyAssigned(Wheel::markAsPersisted, Wheel::isPersisted))
										.add(Wheel::getModel))
									.indexedBy(indexColumn4).mappedBy(Wheel::setVehicle)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)))
								.build(persistenceContext6),
						persistenceContext6.getConnectionProvider() },
//				{	"table per class",
//					entityBuilder(Vehicle.class, LONG_TYPE)
//						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
//							.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
//									.add(Engine::getId).identifier(ALREADY_ASSIGNED))
//						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
//								.addSubClass(subentityBuilder(Car.class)
//										.add(Car::getModel))
//								.addSubClass(subentityBuilder(Truk.class)
//										.add(Truk::getColor)))
//						.build(persistenceContext6),
//						persistenceContext6.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		new DDLDeployer(persistenceContext4).deployDDL();
		new DDLDeployer(persistenceContext5).deployDDL();
		new DDLDeployer(persistenceContext6).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_subClassHasOneToMany_data")
	void crud_polymorphism_subClassHasOneToMany(String testDisplayName, IEntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.addWheel(new Wheel("XYZ-ABC-01"));
		Truk dummyTruk = new Truk(2L);
		dummyTruk.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruk));
		
		Car dummyCarModfied = new Car(1L);
		dummyCarModfied.setModel("Peugeot");
		dummyCarModfied.addWheel(new Wheel("XYZ-ABC-02"));
		Truk dummyTrukModfied = new Truk(2L);
		dummyTrukModfied.setColor(new Color(99));
		
		persister.update(dummyCarModfied, dummyCar, true);
		
		persister.update(dummyTrukModfied, dummyTruk, true);
		
		connectionProvider.getCurrentConnection().commit();
		assertEquals(1, persister.delete(dummyCarModfied));
		assertEquals(1, persister.delete(dummyTrukModfied));
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(2, persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied)));
		
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(dummyTrukModfied, persister.select(dummyTruk.getId()));
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		dummyCarModfied.getWheels().forEach(w -> w.setVehicle(dummyCarModfied));	// this is done only for equality check of reverse setting, because deletion set it to null (which must be fixed, bug see CollecctionUpdater)
		assertEquals(dummyCarModfied, selectedCar);
		// testing afterSelect listener of sub entities relations
		((Car) selectedCar).getWheels().forEach(wheel -> assertTrue(wheel.isPersisted()));
			 
		assertAllEquals(dummyCarModfied.getWheels(), ((Car) selectedCar).getWheels());
		assertEquals(Arrays.asSet(dummyCarModfied, dummyTrukModfied), new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId()))));
	}
	
	static Object[][] polymorphism_subClassHasElementCollection_data() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		Object[][] result = new Object[][] {
//				{	"single table",
//					entityBuilder(Vehicle.class, LONG_TYPE)
//						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
//						.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
//								.add(Engine::getId).identifier(ALREADY_ASSIGNED))
//						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
//								.addSubClass(subentityBuilder(Car.class)
//										.add(Car::getModel), "CAR")
//								.addSubClass(subentityBuilder(Truk.class)
//										.add(Truk::getColor), "TRUK"))
//						.build(persistenceContext1),
//						persistenceContext1.getConnectionProvider() },
				{	"joined tables / one-to-many with association table",
						entityBuilder(Vehicle.class, LONG_TYPE)
								.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
								.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
										.addSubClass(subentityBuilder(Car.class)
												.add(Car::getModel)
												.addCollection(Car::getPlates, String.class))
										.addSubClass(subentityBuilder(Truk.class)
												.add(Truk::getColor)))
								.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
//				{	"table per class",
//					entityBuilder(Vehicle.class, LONG_TYPE)
//						.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
//							.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
//									.add(Engine::getId).identifier(ALREADY_ASSIGNED))
//						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
//								.addSubClass(subentityBuilder(Car.class)
//										.add(Car::getModel))
//								.addSubClass(subentityBuilder(Truk.class)
//										.add(Truk::getColor)))
//						.build(persistenceContext3),
//						persistenceContext3.getConnectionProvider() },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	}
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphism_subClassHasElementCollection_data")
	void crud_polymorphism_subClassHasElementCollection(String testDisplayName, IEntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		dummyCar.addPlate("XYZ-ABC-01");
		Truk dummyTruk = new Truk(2L);
		dummyTruk.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruk));
		
		Car dummyCarModfied = new Car(1L);
		dummyCarModfied.setModel("Peugeot");
		dummyCarModfied.addPlate("XYZ-ABC-02");
		Truk dummyTrukModfied = new Truk(2L);
		dummyTrukModfied.setColor(new Color(99));
		
		persister.update(dummyCarModfied, dummyCar, true);
		
		persister.update(dummyTrukModfied, dummyTruk, true);
		
		connectionProvider.getCurrentConnection().commit();
		assertEquals(1, persister.delete(dummyCarModfied));
		// nothing to delete because all was deleted by cascade
		assertEquals(0, connectionProvider.getCurrentConnection().prepareStatement("delete from Car_plates").executeUpdate());
		assertEquals(1, persister.delete(dummyTrukModfied));
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(2, persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied)));
		
		connectionProvider.getCurrentConnection().rollback();
		
		assertEquals(dummyTrukModfied, persister.select(dummyTruk.getId()));
		AbstractVehicle selectedCar = persister.select(dummyCar.getId());
		assertEquals(dummyCarModfied, selectedCar);
		assertAllEquals(Arrays.asList("XYZ-ABC-02"), ((Car) selectedCar).getPlates());
		assertEquals(Arrays.asSet(dummyCarModfied, dummyTrukModfied), new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId()))));
	}
	
	@Nested
	class OneToSingleTableOne {
		
		@Test
		void oneSubClass() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
							.addSubClass(subentityBuilder(Car.class)
										.add(Car::getId)
										.add(Car::getModel)
										.add(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from Vehicle", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertEquals(Arrays.asList("Renault"), allCars);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertEquals(Arrays.asList("Peugeot"), existingModels);
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertEquals(Collections.emptyList(), existingModels);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void twoSubClasses() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor), "CAR")
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getColor), "TRUK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Duo<String, Integer>> modelQuery = persistenceContext.newQuery("select * from Vehicle", (Class<Duo<String, Integer>>) (Class) Duo.class)
					.mapKey(Duo::new, "model", String.class, "color", Integer.class);
			
			List<Duo<String, Integer>> allCars = modelQuery.execute();
			assertEquals(Arrays.asSet(new Duo<>("Renault", 666), new Duo<>(null, 42)), new HashSet<>(allCars));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<Duo<String, Integer>> existingModels = modelQuery.execute();
			assertEquals(Arrays.asSet(new Duo<>("Peugeot", 666), new Duo<>(null, 42)), new HashSet<>(existingModels));
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);
			
			List<? extends AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertEquals(Arrays.asHashSet(dummyCar, dummyTruk), new HashSet<>(loadedVehicles));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertEquals(Arrays.asSet(new Duo<>(null, 42)), new HashSet<>(existingModels));
			
			abstractVehiclePersister.delete(dummyTruk);
			existingModels = modelQuery.execute();
			assertEquals(Collections.emptyList(), existingModels);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel), "CAR")
							.addSubClass(subentityBuilder(Truk.class),
									"TRUK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='CAR'", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertEquals(Arrays.asList(1), carIds);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='TRUK'", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertEquals(Arrays.asList(2), trukIds);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			int updatedRowCount = abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			assertEquals(1, updatedRowCount);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);
			
			List<? extends Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertEquals(0, carCount);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from Vehicle where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertEquals(0, trukCount);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void listenersAreNotified() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asList(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asList(loadedCar));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
		}
	}
	
	@Nested
	class OneToJoinedTablesOne {
		
		@Test
		void oneSubClass() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Car", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from Vehicle left outer join car on Vehicle.id = car.id", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertEquals(Arrays.asList("Renault"), allCars);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertEquals(Arrays.asList("Peugeot"), existingModels);
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertEquals(Collections.emptyList(), existingModels);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void twoSubClasses() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
									.add(Car::getModel)
									.add(Car::getColor))
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getId)
									.add(Truk::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Car", "Truk", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from Vehicle", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> vehicleIds = vehicleIdQuery.execute();
			assertEquals(Arrays.asList(1, 2), vehicleIds);
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertEquals(Arrays.asList(1), carIds);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertEquals(Arrays.asList(2), trukIds);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);
			
			List<? extends AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertEquals(Arrays.asHashSet(dummyCar, dummyTruk), new HashSet<>(loadedVehicles));
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> vehicleQuery = persistenceContext.newQuery("select"
					+ " count(*) as vehicleCount from Vehicle where id in ("
					+ dummyCar.getId().getSurrogate() + ", " + + dummyTruk.getId().getSurrogate() + ")", Integer.class)
					.mapKey(SerializableFunction.identity(), "vehicleCount", Integer.class);
			
			Integer vehicleCount = Iterables.first(vehicleQuery.execute());
			assertEquals(0, vehicleCount);
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertEquals(0, carCount);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from car where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertEquals(0, trukCount);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		
		@Test
		void twoSubClasses_withCommonProperties() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel))
							.addSubClass(subentityBuilder(Truk.class)
									))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Car", "Truk", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from Vehicle", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> vehicleIds = vehicleIdQuery.execute();
			assertEquals(Arrays.asList(1, 2), vehicleIds);
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertEquals(Arrays.asList(1), carIds);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertEquals(Arrays.asList(2), trukIds);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			int updatedRowCount = abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			assertEquals(1, updatedRowCount);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);
			
			List<? extends Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertEquals(0, carCount);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from Vehicle where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertEquals(0, trukCount);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void listenersAreNotified() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asList(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asList(loadedCar));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
		}
	}
	
	@Nested
	class OneToTablePerClassOne {
		
		@Test
		void oneSubClass() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from car", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertEquals(Arrays.asList("Renault"), allCars);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertEquals(Arrays.asList("Peugeot"), existingModels);
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertEquals(Collections.emptyList(), existingModels);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void twoSubClasses() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor))
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getColor)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car", "Truk", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));

			Truk dummyTruk = new Truk(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruk.setColor(new Color(42));

			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> carIds = carIdQuery.execute();
			assertEquals(Arrays.asList(1), carIds);

			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> trukIds = trukIdQuery.execute();
			assertEquals(Arrays.asList(2), trukIds);

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);

			List<? extends AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertAllEquals(Arrays.asHashSet(dummyCar, dummyTruk), new HashSet<>(loadedVehicles));

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);

			Integer carCount = Iterables.first(carQuery.execute());
			assertEquals(0, carCount);

			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from car where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);

			Integer trukCount = Iterables.first(trukQuery.execute());
			assertEquals(0, trukCount);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel))
							.addSubClass(subentityBuilder(Truk.class)
									))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car", "Truk", "Engine"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class, Engine.class), Iterables.collect(persistenceContext.getPersisters(), IEntityPersister::getClassToPersist, HashSet::new));

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));

			Truk dummyTruk = new Truk(2L);
			dummyCar.setEngine(new Engine(200L));
			dummyTruk.setColor(new Color(42));

			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> carIds = carIdQuery.execute();
			assertEquals(Arrays.asList(1), carIds);

			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> trukIds = trukIdQuery.execute();
			assertEquals(Arrays.asList(2), trukIds);

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedVehicle);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertEquals(dummyTruk, loadedVehicle);

			List<? extends Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));

			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(666))).execute();
			assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);

			Integer carCount = Iterables.first(carQuery.execute());
			assertEquals(0, carCount);

			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from truk where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);

			Integer trukCount = Iterables.first(trukQuery.execute());
			assertEquals(0, trukCount);
			
			// because we asked for orphan removal, engine should not be present anymore
			ExecutableSelect<Long> engineQuery = persistenceContext.newQuery("select id from Engine", Long.class)
					.mapKey(SerializableFunction.identity(), "id", Long.class);
			
			assertEquals(new ArrayList<>(), engineQuery.execute());
		}
		
		@Test
		void listenersAreNotified() {
			IEntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Vehicle::getEngine, entityBuilder(Engine.class, LONG_TYPE)
							.add(Engine::getId).identifier(ALREADY_ASSIGNED))
							.cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setEngine(new Engine(100L));
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.addInsertListener(insertListenerMock);
			abstractVehiclePersister.addUpdateListener(updateListenerMock);
			abstractVehiclePersister.addSelectListener(selectListenerMock);
			abstractVehiclePersister.addDeleteListener(deleteListenerMock);
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			verify(insertListenerMock).beforeInsert(Arrays.asList(dummyCar));
			verify(insertListenerMock).afterInsert(Arrays.asList(dummyCar));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			verify(updateListenerMock).beforeUpdate(any(), eq(true));
			verify(updateListenerMock).afterUpdate(any(), eq(true));
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			verify(selectListenerMock).beforeSelect(Arrays.asList(new PersistedIdentifier<>(1L)));
			verify(selectListenerMock).afterSelect(Arrays.asList(loadedCar));
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			verify(deleteListenerMock).beforeDelete(Arrays.asList(dummyCar));
			verify(deleteListenerMock).afterDelete(Arrays.asList(dummyCar));
		}
	}
	
	@Nested
	class OneToPolymorphicOne {
		
		@Test
		void oneToJoinedTable_crud() {
			IFluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp);
			
			IFluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
									.addSubClass(subentityBuilder(Truk.class))
									.addSubClass(subentityBuilder(Car.class))
							);
			
			IEntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Person person = new Person(1);
			person.setVehicle(new Car(42L));
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// updating one-to-one relation
			person.setVehicle(new Truk(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(42L)));
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(666L)));
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truk(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// testing deletion
			testInstance.delete(person);
			assertNull(testInstance.select(person.getId()));
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(17L)));
		}
		
		@Test
		void oneToJoinedTable_crud_ownedByReverseSide() {
			IFluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp);
			
			IFluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>joinedTables()
									.addSubClass(subentityBuilder(Truk.class))
									.addSubClass(subentityBuilder(Car.class))
							);
			
			IEntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL).mappedBy(Vehicle::getOwner)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			// insert
			Person person = new Person(1);
			person.setVehicle(new Car(42L));
			testInstance.insert(person);
			Person loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// updating one-to-one relation
			person.setVehicle(new Truk(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(42L)));
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(666L)));
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truk(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// testing deletion
			testInstance.delete(person);
			assertNull(testInstance.select(person.getId()));
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(17L)));
		}
		
		@Test
		void oneToSingleTable_crud() {
			IFluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp);
			
			IFluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
							.add(Vehicle::getColor)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
									.addSubClass(subentityBuilder(Truk.class), "T")
									.addSubClass(subentityBuilder(Car.class), "C")
							);
			
			IEntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL)
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
			assertEquals(person, loadedPerson);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficent for ou checking
			// Note htat we don't us ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursivity 
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
			
			Assertions.assertEquals(person, loadedPerson, personPrinter::toString);
			// ensuring that reverse side is also set
			assertEquals(loadedPerson, loadedPerson.getVehicle().getOwner());
			
			// updating one-to-one relation
			person.setVehicle(new Truk(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			Assertions.assertEquals(person, loadedPerson, personPrinter::toString);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(42L)));
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(666L)));
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truk(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// testing deletion
			testInstance.delete(person);
			assertNull(testInstance.select(person.getId()));
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(17L)));
		}
		
		@Test
		void oneToSingleTable_crud_ownedByReverseSide() {
			IFluentEmbeddableMappingBuilder<Person> timestampedPersistentBeanMapping =
					embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp);
			
			IFluentEntityMappingBuilder<Vehicle, Identifier<Long>> vehicleConfiguration =
					entityBuilder(Vehicle.class, LONG_TYPE)
							.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
							.add(Vehicle::getColor)
							.mapPolymorphism(PolymorphismPolicy.<Vehicle, Identifier<Long>>singleTable()
									.addSubClass(subentityBuilder(Truk.class), "T")
									.addSubClass(subentityBuilder(Car.class), "C")
							);
			
			IEntityPersister<Person, Identifier<Long>> testInstance = entityBuilder(Person.class, LONG_TYPE)
					.add(Person::getId).identifier(ALREADY_ASSIGNED)
					.addOneToOne(Person::getVehicle, vehicleConfiguration).cascading(RelationMode.ALL_ORPHAN_REMOVAL).mappedBy(Vehicle::getOwner)
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
			assertEquals(person, loadedPerson);
			
			// updating embedded value
			person.setTimestamp(new Timestamp());
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficent for ou checking
			// Note htat we don't us ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursivity 
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
			
			Assertions.assertEquals(person, loadedPerson, personPrinter::toString);
			// ensuring that reverse side is also set
			assertEquals(loadedPerson, loadedPerson.getVehicle().getOwner());
			
			// updating one-to-one relation
			person.setVehicle(new Truk(666L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			Assertions.assertEquals(person, loadedPerson, personPrinter::toString);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(42L)));
			
			// nullifying one-to-one relation
			person.setVehicle(null);
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(666L)));
			
			
			// setting new one-to-one relation
			person.setVehicle(new Truk(17L));
			testInstance.update(person, loadedPerson, true);
			
			loadedPerson = testInstance.select(person.getId());
			assertEquals(person, loadedPerson);
			
			// testing deletion
			testInstance.delete(person);
			assertNull(testInstance.select(person.getId()));
			// checking for orphan removal (relation was marked as such)
			assertNull(persistenceContext.getPersister(Vehicle.class).select(new PersistedIdentifier<>(17L)));
		}
		
	}
	
	
	
	
	@Nested
	class OneToPolymorphicMany {
		
		@Test
		void oneToJoinedTables_crud_withAssociationTable() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>joinedTables()
									.addSubClass(subentityBuilder(Village.class)
										.add(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
										.add(Town::getDiscotecCount))
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
							.add(Country::getId).identifier(ALREADY_ASSIGNED)
							.addOneToManySet(Country::getCities, cityConfiguration)
								.reverselySetBy(City::setCountry)	// necessary if you want bidirectionnality to be set in memory
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
			assertEquals(country, loadedCountry);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select Country_id from Country_cities", Long.class)
					.mapKey(i -> i, "Country_id", Long.class)
					.execute();
			assertEquals(Arrays.asList(country.getId().getSurrogate()), cityCountryIds);

			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from city", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
		}
		
		@Test
		void oneToJoinedTables_crud_ownedByReverseSide() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>joinedTables()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount))
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
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
			assertEquals(country, loadedCountry);
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficent for ou checking
			// Note htat we don't us ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursivity 
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
			
			assertAllEquals(country.getCities(), loadedCountry.getCities(), cityPrinter::toString);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select CountryId from City", Long.class)
					.mapKey(i -> i, "CountryId", Long.class)
					.execute();
			assertEquals(Arrays.asSet(country.getId().getSurrogate(), null), new HashSet<>(cityCountryIds));
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from city", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
		}
		
		@Test
		void oneToSingleTable_crud_withAssociationTable() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>singleTable()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount), "V")
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount), "T")
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
						.reverselySetBy(City::setCountry)	// necessary if you want bidirectionnality to be set in memory
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
			assertEquals(country, loadedCountry);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select Country_id from Country_cities", Long.class)
					.mapKey(i -> i, "Country_id", Long.class)
					.execute();
			assertEquals(Arrays.asList(country.getId().getSurrogate()), cityCountryIds);
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from city", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
		}
		
		@Test
		void oneToSingleTable_crud_ownedByReverseSide() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>singleTable()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount), "V")
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount), "T")
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
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
			assertEquals(country, loadedCountry);
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficent for ou checking
			// Note htat we don't us ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursivity 
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
			
			assertAllEquals(country.getCities(), loadedCountry.getCities(), cityPrinter::toString);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId().getSurrogate());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select CountryId from City", Long.class)
					.mapKey(i -> i, "CountryId", Long.class)
					.execute();
			assertEquals(Arrays.asSet(country.getId().getSurrogate(), null), new HashSet<>(cityCountryIds));
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from city", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
		}
		
		@Test
		void oneToTablePerClass_crud_withAssociationTable() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>tablePerClass()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount))
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
						.reverselySetBy(City::setCountry)	// necessary if you want bidirectionnality to be set in memory
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
			assertEquals(country, loadedCountry);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select Country_id from Country_cities", Long.class)
					.mapKey(i -> i, "Country_id", Long.class)
					.execute();
			assertEquals(Arrays.asList(country.getId().getSurrogate()), cityCountryIds);
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from Town union all select id from Village", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
		}
		
		@Test
		void oneToTablePerClass_crud_ownedByReverseSide_foreignKeysAreCreated() throws SQLException {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>tablePerClass()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount))
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
					.mappedBy(City::getCountry)
					.cascading(RelationMode.ALL)
					.mapSuperClass(timestampedPersistentBeanMapping)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Connection currentConnection = persistenceContext.getConnectionProvider().getCurrentConnection();
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
			assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
			
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
			assertEquals(expectedForeignKey.getSignature(), foundForeignKey.getSignature());
		}
			
		@Test
		void oneToTablePerClass_crud_ownedByReverseSide() {
			
			IFluentEmbeddableMappingBuilder<Country> timestampedPersistentBeanMapping =
					embeddableBuilder(Country.class)
							.add(Country::getName)
							.embed(Country::getTimestamp);
			
			IFluentEntityMappingBuilder<City, Identifier<Long>> cityConfiguration =
					entityBuilder(City.class, LONG_TYPE)
							.add(City::getId).identifier(ALREADY_ASSIGNED)
							.add(City::getName)
							.mapPolymorphism(PolymorphismPolicy.<City, Identifier<Long>>tablePerClass()
									.addSubClass(subentityBuilder(Village.class)
											.add(Village::getBarCount))
									.addSubClass(subentityBuilder(Town.class)
											.add(Town::getDiscotecCount))
							);
			
			IEntityPersister<Country, Identifier<Long>> testInstance = entityBuilder(Country.class, LONG_TYPE)
					.add(Country::getId).identifier(ALREADY_ASSIGNED)
					.addOneToManySet(Country::getCities, cityConfiguration)
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
			assertEquals(country, loadedCountry);
			
			// we use a printer to compare our results because entities override equals() which only keep "id" into account
			// which is far from sufficent for ou checking
			// Note htat we don't us ObjectPrinterBuilder#printerFor because it take getCities() into account whereas its code is not ready for recursivity 
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
			
			assertAllEquals(country.getCities(), loadedCountry.getCities(), cityPrinter::toString);
			// ensuring that reverse side is also set
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update and select of mixed type of City
			Town lyon = new Town(17L);
			lyon.setDiscotecCount(123);
			lyon.setName("Lyon");
			country.addCity(lyon);
			grenoble.setBarCount(51);
			testInstance.update(country, loadedCountry, true);
			
			loadedCountry = testInstance.select(country.getId());
			// resulting select must contain Town and Village
			assertEquals(Arrays.asHashSet(grenoble, lyon), loadedCountry.getCities());
			// bidirectionality must be preserved
			assertEquals(Arrays.asHashSet(loadedCountry), Iterables.collect(loadedCountry.getCities(), City::getCountry, HashSet::new));
			
			// testing update : removal of a city, reversed column must be set to null
			Country modifiedCountry = new Country(country.getId().getSurrogate());
			modifiedCountry.addCity(Iterables.first(country.getCities()));
			
			testInstance.update(modifiedCountry, country, false);
			// there's only 1 relation in table
			List<Long> cityCountryIds = persistenceContext.newQuery("select countryId from Town union all select countryId from Village", Long.class)
					.mapKey(i -> i, "countryId", Long.class)
					.execute();
			assertEquals(Arrays.asSet(country.getId().getSurrogate(), null), new HashSet<>(cityCountryIds));
			
			// testing delete
			testInstance.delete(modifiedCountry);
			// Cities shouldn't be deleted (we didn't ask for delete orphan)
			List<Long> cityIds = persistenceContext.newQuery("select id from Town union all select id from Village", Long.class)
					.mapKey(i -> i, "id", Long.class)
					.execute();
			assertEquals(Arrays.asSet(grenoble.getId().getSurrogate(), lyon.getId().getSurrogate()), new HashSet<>(cityIds));
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
