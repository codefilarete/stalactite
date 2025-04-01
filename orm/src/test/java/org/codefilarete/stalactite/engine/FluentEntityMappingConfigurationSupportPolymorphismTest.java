package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyQueryMapper;
import org.codefilarete.stalactite.engine.listener.DeleteListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.PersistListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Car;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.engine.model.Engine;
import org.codefilarete.stalactite.engine.model.Truck;
import org.codefilarete.stalactite.engine.model.Vehicle;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialectBuilder;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.engine.MappingEase.subentityBuilder;
import static org.codefilarete.stalactite.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
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
class FluentEntityMappingConfigurationSupportPolymorphismTest {
	
	private static final Dialect DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new CurrentThreadConnectionProvider(dataSource);
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
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
	}
	
	
	static Object[][] polymorphicPersisters() {
		PersistenceContext persistenceContext1 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext2 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		PersistenceContext persistenceContext3 = new PersistenceContext(new HSQLDBInMemoryDataSource(), DIALECT);
		Object[][] result = new Object[][] {
				{	"single table",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getId)
										.map(Car::getModel), "CAR")
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getId)
										.map(Truck::getColor), "TRUCK"))
						.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"joined tables",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getId)
										.map(Car::getModel))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getId)
										.map(Truck::getColor)))
						.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"table per class",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
								.addSubClass(subentityBuilder(Car.class)
										.map(Car::getId)
										.map(Car::getModel))
								.addSubClass(subentityBuilder(Truck.class)
										.map(Truck::getId)
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
	@MethodSource("polymorphicPersisters")
	void crud(String testDisplayName, EntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		Truck dummyTruck = new Truck(2L);
		dummyTruck.setColor(new Color(42));
		
		// insert test
		persister.insert(Arrays.asList(dummyCar, dummyTruck));
		
		Car dummyCarModified = new Car(1L);
		dummyCarModified.setModel("Peugeot");
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
	
	@Nested
	class SingleTable {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
								.map(Car::getId)
								.map(Car::getModel)
								.map(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("AbstractVehicle");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", String.class)
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
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
							// mapped super class defines id
							.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
									.addSubClass(subentityBuilder(Car.class)
											.map(Car::getId)
											.map(Car::getModel)
											.map(Car::getColor), "CAR")
									.addSubClass(subentityBuilder(Truck.class)
											.map(Truck::getId)
											.map(Truck::getColor), "TRUCK"))
							.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("AbstractVehicle");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Duo<String, Integer>> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", (Class<Duo<String, Integer>>) (Class) Duo.class)
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
			
			Set<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruck);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).containsExactlyInAnyOrder(new Duo<>(null, 42));
			
			abstractVehiclePersister.delete(dummyTruck);
			existingModels = modelQuery.execute(Accumulators.toSet());
			assertThat(existingModels).isEmpty();
		}
		
		@Test
		void twoSubClasses_withReadonlyProperty() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel).readonly()
									.map(Car::getColor), "CAR")
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId)
									.map(Truck::getColor), "TRUCK"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<String> vehicleIdQuery = persistenceContext.newQuery("select model from AbstractVehicle", String.class)
					.mapKey("model", String.class);
			
			String model = vehicleIdQuery.execute(Accumulators.getFirst());
			assertThat(model).isNull();
		}
		
		@Test
		void twoSubClasses_withSqlBinder() {
			// we create a local dialect to avoid conflict with sqlBinder(..) usage and ensure the test has the good context
			Dialect dialect = HSQLDBDialectBuilder.defaultHSQLDBDialect();
			dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
			dialect.getSqlTypeRegistry().put(Identifier.class, "int");
			PersistenceContext persistenceContext = new PersistenceContext(connectionProvider, dialect);
			
			Table<?> abstractVehicleTable = new Table<>("AbstractVehicle");
			// we define the column, else it has no type and the
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE, abstractVehicleTable)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor)
											.sqlBinder(INTEGER_PRIMITIVE_BINDER.wrap(Color::new, Color::getRgb))
									, "CAR")
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId), "TRUCK"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			assertThat(((Car) abstractVehiclePersister.select(dummyCar.getId())).getColor()).isEqualTo(new Color(666));
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel), "CAR")
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId), "TRUCK"))
					.build(persistenceContext);
			
			// Schema contains only one table : parent class one
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
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
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getDelegate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from Vehicle where id = " + dummyTruck.getId().getDelegate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor), "CAR"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
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
	class JoinTable {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).isEqualTo(Arrays.asHashSet("AbstractVehicle", "Car"));
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			abstractVehiclePersister.insert(dummyCar);
			
			ExecutableBeanPropertyQueryMapper<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle left outer join car on abstractVehicle.id = car.id", String.class)
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
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
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
			assertThat(tables).containsExactlyInAnyOrder("AbstractVehicle", "Car", "Truck");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleIdQuery = persistenceContext.newQuery("select id from abstractVehicle", Integer.class)
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
			
			Set<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruck);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> vehicleQuery = persistenceContext.newQuery("select"
					+ " count(*) as vehicleCount from abstractVehicle where id in ("
					+ dummyCar.getId().getDelegate() + ", " + + dummyTruck.getId().getDelegate() + ")", Integer.class)
					.mapKey("vehicleCount", Integer.class);
			
			Integer vehicleCount = Iterables.first(vehicleQuery.execute(Accumulators.toSet()));
			assertThat(vehicleCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getDelegate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from car where id = " + dummyTruck.getId().getDelegate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
		}
		
		@Test
		void twoSubClasses_withReadonlyProperty() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel).readonly()
									.map(Car::getColor))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId)
									.map(Truck::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<String> vehicleIdQuery = persistenceContext.newQuery("select model from Car", String.class)
					.mapKey("model", String.class);
			
			String model = vehicleIdQuery.execute(Accumulators.getFirst());
			assertThat(model).isNull();
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getId)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Car", "Truck");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
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
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getDelegate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);
			
			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from Vehicle where id = " + dummyTruck.getId().getDelegate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
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
	class TablePerClass {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
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
		}
		
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel)
									.map(Car::getColor))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getColor)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truck");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));

			Truck dummyTruck = new Truck(2L);
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
			
			Set<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruck, dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getDelegate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);

			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from car where id = " + dummyTruck.getId().getDelegate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
		}
		
		@Test
		void twoSubClasses_withReadonlyProperty() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel).readonly()
									.map(Car::getColor))
							.addSubClass(subentityBuilder(Truck.class)
									.map(Truck::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			Truck dummyTruck = new Truck(2L);
			dummyTruck.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruck));
			
			ExecutableBeanPropertyQueryMapper<String> vehicleIdQuery = persistenceContext.newQuery("select model from Car", String.class)
					.mapKey("model", String.class);
			
			String model = vehicleIdQuery.execute(Accumulators.getFirst());
			assertThat(model).isNull();
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getModel))
							.addSubClass(subentityBuilder(Truck.class)
									))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truck");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);

			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();

			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));

			Truck dummyTruck = new Truck(2L);
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
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getDelegate(), Integer.class)
					.mapKey("carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute(Accumulators.toSet()));
			assertThat(carCount).isEqualTo(0);

			ExecutableBeanPropertyQueryMapper<Integer> truckQuery = persistenceContext.newQuery("select"
					+ " count(*) as truckCount from truck where id = " + dummyTruck.getId().getDelegate(), Integer.class)
					.mapKey("truckCount", Integer.class);
			
			Integer truckCount = Iterables.first(truckQuery.execute(Accumulators.toSet()));
			assertThat(truckCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.map(Car::getId)
									.map(Car::getModel)
									.map(Car::getColor)))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
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
	
	static class ElectricCar extends Car {
		
		private ElectricPlug plug;
		
		ElectricCar() {
			super();
		}
		
		ElectricCar(long id) {
			super(id);
		}
		
		public ElectricPlug getPlug() {
			return plug;
		}
		
		public ElectricCar setPlug(ElectricPlug plug) {
			this.plug = plug;
			return this;
		}
	}
	
	enum ElectricPlug {
		Type1,
		Type2,
		CCS,
		CHAdeMo
	}
	
}
