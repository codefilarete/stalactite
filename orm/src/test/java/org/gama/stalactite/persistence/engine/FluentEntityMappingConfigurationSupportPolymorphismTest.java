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
import org.gama.stalactite.persistence.engine.PersistenceContext.ExecutableSelect;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.model.AbstractVehicle;
import org.gama.stalactite.persistence.engine.model.Car;
import org.gama.stalactite.persistence.engine.model.Color;
import org.gama.stalactite.persistence.engine.model.Truk;
import org.gama.stalactite.persistence.engine.model.Vehicle;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.gama.stalactite.persistence.engine.MappingEase.subentityBuilder;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.gama.stalactite.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
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
		DIALECT.getSqlTypeRegistry().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		DIALECT.getSqlTypeRegistry().put(Color.class, "int");
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
				{	"single table",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getId)
										.add(Car::getModel), "CAR")
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getId)
										.add(Truk::getColor), "TRUK"))
						.build(persistenceContext1),
						persistenceContext1.getConnectionProvider() },
				{	"joined tables",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getId)
										.add(Car::getModel))
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getId)
										.add(Truk::getColor)))
						.build(persistenceContext2),
						persistenceContext2.getConnectionProvider() },
				{	"table per class",
					entityBuilder(AbstractVehicle.class, LONG_TYPE)
						.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
						.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
								.addSubClass(subentityBuilder(Car.class)
										.add(Car::getId)
										.add(Car::getModel))
								.addSubClass(subentityBuilder(Truk.class)
										.add(Truk::getId)
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
	@MethodSource("polymorphicPersisters")
	void crud(String testDisplayName, EntityPersister<AbstractVehicle, Identifier<Long>> persister, ConnectionProvider connectionProvider) throws SQLException {
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
		
		connectionProvider.getCurrentConnection().commit();
		assertThat(persister.delete(dummyCarModfied)).isEqualTo(1);
		assertThat(persister.delete(dummyTrukModfied)).isEqualTo(1);
		connectionProvider.getCurrentConnection().rollback();
		
		assertThat(persister.delete(Arrays.asList(dummyCarModfied, dummyTrukModfied))).isEqualTo(2);
		
		connectionProvider.getCurrentConnection().rollback();
		
		assertThat(persister.select(dummyTruk.getId())).isEqualTo(dummyTrukModfied);
		assertThat(persister.select(dummyCar.getId())).isEqualTo(dummyCarModfied);
		assertThat(new HashSet<>(persister.select(Arrays.asSet(dummyCar.getId(), dummyTruk.getId())))).isEqualTo(Arrays.asSet(dummyCarModfied,
				dummyTrukModfied));
	}
	
	@Nested
	class SingleTable {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
								.add(Car::getId)
								.add(Car::getModel)
								.add(Car::getColor), "CAR"))
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
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertThat(existingModels).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
							// mapped super class defines id
							.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
							.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
									.addSubClass(subentityBuilder(Car.class)
											.add(Car::getId)
											.add(Car::getModel)
											.add(Car::getColor), "CAR")
									.addSubClass(subentityBuilder(Truk.class)
											.add(Truk::getId)
											.add(Truk::getColor), "TRUK"))
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
			
			Truk dummyTruk = new Truk(2L);
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Duo<String, Integer>> modelQuery = persistenceContext.newQuery("select * from abstractVehicle", (Class<Duo<String, Integer>>) (Class) Duo.class)
					.mapKey(Duo::new, "model", String.class, "color", Integer.class);
			
			List<Duo<String, Integer>> allCars = modelQuery.execute();
			assertThat(allCars).containsExactlyInAnyOrder(new Duo<>("Renault", 666), new Duo<>(null, 42));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<Duo<String, Integer>> existingModels = modelQuery.execute();
			assertThat(existingModels).containsExactlyInAnyOrder(new Duo<>("Peugeot", 666), new Duo<>(null, 42));
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);
			
			List<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruk);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertThat(existingModels).containsExactlyInAnyOrder(new Duo<>(null, 42));
			
			abstractVehiclePersister.delete(dummyTruk);
			existingModels = modelQuery.execute();
			assertThat(existingModels).isEmpty();
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
									.add(Car::getModel), "CAR")
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getId), "TRUK"))
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
			
			Truk dummyTruk = new Truk(2L);
			dummyTruk.setColor(new Color(42));
			
			// insert test
			abstractVehiclePersister.insert(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='CAR'", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertThat(carIds).containsExactly(1);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from Vehicle where DTYPE ='TRUK'", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertThat(trukIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			int updatedRowCount = abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			assertThat(updatedRowCount).isEqualTo(1);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);
			
			List<Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruk);
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertThat(carCount).isEqualTo(0);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from Vehicle where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertThat(trukCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>singleTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
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
	class JoinTable {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
									.add(Car::getModel)
									.add(Car::getColor)))
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
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from abstractVehicle left outer join car on abstractVehicle.id = car.id", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertThat(allCars).containsExactly("Renault");
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertThat(existingModels).isEmpty();
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
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
			assertThat(tables).containsExactlyInAnyOrder("AbstractVehicle", "Car", "Truk");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);
			
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
			assertThat(vehicleIds).containsExactly(1, 2);
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertThat(carIds).containsExactly(1);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertThat(trukIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);
			
			List<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar, dummyTruk);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> vehicleQuery = persistenceContext.newQuery("select"
					+ " count(*) as vehicleCount from abstractVehicle where id in ("
					+ dummyCar.getId().getSurrogate() + ", " + + dummyTruk.getId().getSurrogate() + ")", Integer.class)
					.mapKey(SerializableFunction.identity(), "vehicleCount", Integer.class);
			
			Integer vehicleCount = Iterables.first(vehicleQuery.execute());
			assertThat(vehicleCount).isEqualTo(0);
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertThat(carCount).isEqualTo(0);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from car where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertThat(trukCount).isEqualTo(0);
		}
		
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
									.add(Car::getModel))
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getId)))
					.build(persistenceContext);
			
			// Schema contains main and children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Vehicle", "Car", "Truk");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);
			
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
			assertThat(vehicleIds).containsExactly(1, 2);
			
			ExecutableSelect<Integer> carIdQuery = persistenceContext.newQuery("select id from car", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> carIds = carIdQuery.execute();
			assertThat(carIds).containsExactly(1);
			
			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);
			
			List<Integer> trukIds = trukIdQuery.execute();
			assertThat(trukIds).containsExactly(2);
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			
			// update test by modifying only parent property
			dummyCar.setColor(new Color(256));
			int updatedRowCount = abstractVehiclePersister.update(dummyCar, loadedVehicle, false);
			assertThat(updatedRowCount).isEqualTo(1);
			
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);
			
			List<Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruk);
			
			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(256))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));
			
			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from Vehicle where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);
			
			Integer carCount = Iterables.first(carQuery.execute());
			assertThat(carCount).isEqualTo(0);
			
			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from Vehicle where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);
			
			Integer trukCount = Iterables.first(trukQuery.execute());
			assertThat(trukCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>joinTable()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
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
	class TablePerClass {
		
		@Test
		void oneSubClass() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor)))
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
			
			ExecutableSelect<String> modelQuery = persistenceContext.newQuery("select * from car", String.class)
					.mapKey(SerializableFunction.identity(), "model", String.class);
			
			List<String> allCars = modelQuery.execute();
			assertThat(allCars).isEqualTo(Arrays.asList("Renault"));
			
			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);
			
			List<String> existingModels = modelQuery.execute();
			assertThat(existingModels).containsExactly("Peugeot");
			
			// select test
			AbstractVehicle loadedCar = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// delete test
			abstractVehiclePersister.delete(dummyCar);
			
			existingModels = modelQuery.execute();
			assertThat(existingModels).isEqualTo(Collections.emptyList());
		}
		
		@Test
		void twoSubClasses() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel)
									.add(Car::getColor))
							.addSubClass(subentityBuilder(Truk.class)
									.add(Truk::getColor)))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truk");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(AbstractVehicle.class);

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
			assertThat(carIds).isEqualTo(Arrays.asList(1));

			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> trukIds = trukIdQuery.execute();
			assertThat(trukIds).isEqualTo(Arrays.asList(2));

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			AbstractVehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);

			List<AbstractVehicle> loadedVehicles = abstractVehiclePersister.selectAll();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruk, dummyCar);

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);

			Integer carCount = Iterables.first(carQuery.execute());
			assertThat(carCount).isEqualTo(0);

			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from car where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);

			Integer trukCount = Iterables.first(trukQuery.execute());
			assertThat(trukCount).isEqualTo(0);
		}
		
		@Test
		void twoSubClasses_withCommonProperties() {
			EntityPersister<Vehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.mapPolymorphism(PolymorphismPolicy.<Vehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getModel))
							.addSubClass(subentityBuilder(Truk.class)
									))
					.build(persistenceContext);

			// Schema contains children tables
			HashSet<String> tables = Iterables.collect(DDLDeployer.collectTables(persistenceContext), Table::getName, HashSet::new);
			assertThat(tables).containsExactlyInAnyOrder("Car", "Truk");
			
			// Subclasses are not present in context (because doing so they would be accessible but without wrong behavior since some are configured on parent's persister)
			assertThat(persistenceContext.getPersisters()).extracting(EntityPersister::getClassToPersist).containsExactlyInAnyOrder(Vehicle.class);

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
			assertThat(carIds).containsExactly(1);

			ExecutableSelect<Integer> trukIdQuery = persistenceContext.newQuery("select id from truk", Integer.class)
					.mapKey(SerializableFunction.identity(), "id", Integer.class);

			List<Integer> trukIds = trukIdQuery.execute();
			assertThat(trukIds).containsExactly(2);

			// update test
			dummyCar.setModel("Peugeot");
			abstractVehiclePersister.persist(dummyCar);

			// select test
			Vehicle loadedVehicle;
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedVehicle).isEqualTo(dummyCar);
			loadedVehicle = abstractVehiclePersister.select(new PersistedIdentifier<>(2L));
			assertThat(loadedVehicle).isEqualTo(dummyTruk);

			List<Vehicle> loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(42))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyTruk);

			loadedVehicles = abstractVehiclePersister.selectWhere(Vehicle::getColor, Operators.eq(new Color(666))).execute();
			assertThat(loadedVehicles).containsExactlyInAnyOrder(dummyCar);

			// delete test
			abstractVehiclePersister.delete(Arrays.asList(dummyCar, dummyTruk));

			ExecutableSelect<Integer> carQuery = persistenceContext.newQuery("select"
					+ " count(*) as carCount from car where id = " + dummyCar.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "carCount", Integer.class);

			Integer carCount = Iterables.first(carQuery.execute());
			assertThat(carCount).isEqualTo(0);

			ExecutableSelect<Integer> trukQuery = persistenceContext.newQuery("select"
					+ " count(*) as trukCount from truk where id = " + dummyTruk.getId().getSurrogate(), Integer.class)
					.mapKey(SerializableFunction.identity(), "trukCount", Integer.class);

			Integer trukCount = Iterables.first(trukQuery.execute());
			assertThat(trukCount).isEqualTo(0);
		}
		
		@Test
		void listenersAreNotified() {
			EntityPersister<AbstractVehicle, Identifier<Long>> abstractVehiclePersister = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(ALREADY_ASSIGNED)
					.mapPolymorphism(PolymorphismPolicy.<AbstractVehicle>tablePerClass()
							.addSubClass(subentityBuilder(Car.class)
									.add(Car::getId)
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
