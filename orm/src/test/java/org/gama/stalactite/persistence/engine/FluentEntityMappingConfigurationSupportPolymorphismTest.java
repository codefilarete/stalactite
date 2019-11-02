package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.test.Assertions;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.AbstractVehicle;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Car;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Color;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Truk;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Vehicle;
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.LambdaParameterBinder;
import org.gama.stalactite.sql.binder.NullAwareParameterBinder;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy.ALREADY_ASSIGNED;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportPolymorphismTest {
	
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
	
	
	static Object[][] polymorphicPersisters() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new JdbcConnectionProvider(new HSQLDBInMemoryDataSource()), DIALECT);
		Object[][] result = new Object[][] {
//				{	"single table",
//					entityBuilder(AbstractVehicle.class, LONG_TYPE)
//						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
//						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
//								.addSubClass(entityBuilder(Car.class, LONG_TYPE)
//										.add(Car::getId).identifier(ALREADY_ASSIGNED)
//										.add(Car::getModel), "CAR")
//								.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
//										.add(Truk::getId).identifier(ALREADY_ASSIGNED)
//										.add(Truk::getColor), "TRUK"))
//						.build(persistenceContext1) },
				{	"joined tables",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>joinedTables()
								.addSubClass(entityBuilder(Car.class, LONG_TYPE)
										.add(Car::getId).identifier(ALREADY_ASSIGNED)
										.add(Car::getModel))
								.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
										.add(Truk::getId).identifier(ALREADY_ASSIGNED)
										.add(Truk::getColor)))
						.build(persistenceContext2) },
				{	"table per class",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
								.addSubClass(entityBuilder(Car.class, LONG_TYPE)
										.add(Car::getId).identifier(ALREADY_ASSIGNED)
										.add(Car::getModel))
								.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
										.add(Truk::getId).identifier(ALREADY_ASSIGNED)
										.add(Truk::getColor)))
						.build(persistenceContext3) },
		};
		new DDLDeployer(persistenceContext1).deployDDL();
		new DDLDeployer(persistenceContext2).deployDDL();
		new DDLDeployer(persistenceContext3).deployDDL();
		return result;
	} 
	
	
	@ParameterizedTest(name="{0}")
	@MethodSource("polymorphicPersisters")
	void crud(String testDisplayName, Persister<AbstractVehicle, Identifier<Long>, ?> persister) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		Truk dummyTruk = new Truk(2L);
		dummyTruk.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruk));
		
		Car dummyCarModfied = new Car(1L);
		dummyCarModfied.setModel("Peugeot");
		Truk dummyTrukModfied = new Truk(2L);
		dummyTrukModfied.setColor(new Color(99));
		
		persister.update(dummyCarModfied, dummyCar, true);
		
		persister.update(dummyTrukModfied, dummyTruk, true);
		
		persister.getConnectionProvider().getCurrentConnection().commit();
		assertEquals(1, persister.delete(dummyCarModfied));
		assertEquals(1, persister.delete(dummyTrukModfied));
		persister.getConnectionProvider().getCurrentConnection().rollback();
		
		assertEquals(2, persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied)));
		
		persister.getConnectionProvider().getCurrentConnection().rollback();
		
		assertEquals(dummyTrukModfied, persister.select(dummyTruk.getId()));
		assertEquals(dummyCarModfied, persister.select(dummyCar.getId()));
		assertEquals(Arrays.asSet(dummyCarModfied, dummyTrukModfied), new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId()))));
	}
	
	@Nested
	class SingleTable {
		
		@Test
		void oneSubClass() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
					.add(Car::getId).identifier(ALREADY_ASSIGNED)
					.add(Car::getModel)
					.add(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("AbstractVehicle"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", String.class)
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
		}
		
		@Test
		void twoSubClasses() {
			JoinedTablesPersister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
							// mapped super class defines id
							.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
									.addSubClass(entityBuilder(Car.class, LONG_TYPE)
											.add(Car::getId).identifier(ALREADY_ASSIGNED)
											.add(Car::getModel)
											.add(Car::getColor), "CAR")
									.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
											.add(Truk::getId).identifier(ALREADY_ASSIGNED)
											.add(Truk::getColor), "TRUK"))
							.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("AbstractVehicle"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Duo<String, Integer>> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", (Class<Duo<String, Integer>>) (Class) Duo.class)
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
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			JoinedTablesPersister<Vehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel), "CAR")
							.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
									.add(Truk::getId).identifier(ALREADY_ASSIGNED), "TRUK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
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
			Assertions.assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			Assertions.assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));
			
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
		}
		
		@Test
		void listenersAreNotified() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>singleTable()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.getPersisterListener().addInsertListener(insertListenerMock);
			abstractVehiclePersister.getPersisterListener().addUpdateListener(updateListenerMock);
			abstractVehiclePersister.getPersisterListener().addSelectListener(selectListenerMock);
			abstractVehiclePersister.getPersisterListener().addDeleteListener(deleteListenerMock);
			
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
	class JoinedTables {
		
		@Test
		void oneSubClass() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>joinedTables()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("AbstractVehicle", "Car"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle left outer join car on abstractVehicle.id = car.id", String.class)
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
		}
		
		@Test
		void twoSubClasses() {
			JoinedTablesPersister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>joinedTables()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor))
							.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
									.add(Truk::getId).identifier(ALREADY_ASSIGNED)
									.add(Truk::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("AbstractVehicle", "Car", "Truk"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from abstractVehicle", Integer.class)
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
					+ " count(*) as vehicleCount from abstractVehicle where id in ("
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
		}
		
		
		@Test
		void twoSubClasses_withCommonProperties() {
			JoinedTablesPersister<Vehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>joinedTables()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel))
							.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
									.add(Truk::getId).identifier(ALREADY_ASSIGNED)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Vehicle", "Car", "Truk"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truk dummyTruk = new Truk(2L);
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
			Assertions.assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			Assertions.assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));
			
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
		}
		
		@Test
		void listenersAreNotified() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>joinedTables()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.getPersisterListener().addInsertListener(insertListenerMock);
			abstractVehiclePersister.getPersisterListener().addUpdateListener(updateListenerMock);
			abstractVehiclePersister.getPersisterListener().addSelectListener(selectListenerMock);
			abstractVehiclePersister.getPersisterListener().addDeleteListener(deleteListenerMock);
			
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
	class TablePerClass {
		
		@Test
		void oneSubClass() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
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
		}
		
		@Test
		void twoSubClasses() {
			JoinedTablesPersister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor))
							.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
									.add(Truk::getId).identifier(ALREADY_ASSIGNED)
									.add(Truk::getColor)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car", "Truk"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(AbstractVehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));

			Truk dummyTruk = new Truk(2L);
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
			Assertions.assertAllEquals(Arrays.asHashSet(dummyCar, dummyTruk), new HashSet<>(loadedVehicles));

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
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			JoinedTablesPersister<Vehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel))
							.addSubClass(entityBuilder(Truk.class, LONG_TYPE)
									.add(Truk::getId).identifier(ALREADY_ASSIGNED)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertEquals(Arrays.asHashSet("Car", "Truk"), tables);
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertEquals(Arrays.asHashSet(Vehicle.class), Iterables.collect(persistenceContext.getPersisters(), p -> p.getMappingStrategy().getClassToPersist(), HashSet::new));

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));

			Truk dummyTruk = new Truk(2L);
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
			Assertions.assertAllEquals(Arrays.asHashSet(dummyTruk), new HashSet<>(loadedVehicles));

			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(666))).execute();
			Assertions.assertAllEquals(Arrays.asHashSet(dummyCar), new HashSet<>(loadedVehicles));

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
		}
		
		@Test
		void listenersAreNotified() {
			Persister<AbstractVehicle, Identifier<Long>, ?> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle, Identifier<Long>>tablePerClass()
							.addSubClass(entityBuilder(Car.class, LONG_TYPE)
									.add(Car::getId).identifier(ALREADY_ASSIGNED)
									.add(Car::getModel)
									.add(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			InsertListener insertListenerMock = mock(InsertListener.class);
			UpdateListener updateListenerMock = mock(UpdateListener.class);
			SelectListener selectListenerMock = mock(SelectListener.class);
			DeleteListener deleteListenerMock = mock(DeleteListener.class);
			abstractVehiclePersister.getPersisterListener().addInsertListener(insertListenerMock);
			abstractVehiclePersister.getPersisterListener().addUpdateListener(updateListenerMock);
			abstractVehiclePersister.getPersisterListener().addSelectListener(selectListenerMock);
			abstractVehiclePersister.getPersisterListener().addDeleteListener(deleteListenerMock);
			
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
}
