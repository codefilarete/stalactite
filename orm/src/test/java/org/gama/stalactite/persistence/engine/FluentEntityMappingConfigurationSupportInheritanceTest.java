package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.gama.sql.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.gama.sql.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;
import static org.gama.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.gama.stalactite.persistence.id.Identifier.identifierBinder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Guillaume Mary
 */
class FluentEntityMappingConfigurationSupportInheritanceTest {
	
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
		
	@Nested
	class MappedSuperClass {
		
		@Test
		void simpleCase() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)    // note : we don't need to embed Color because it is defined in the Dialect registry
					// concrete class defines id
					.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.add(Vehicle::getColor).getConfiguration())
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertFalse(tables.contains(mappedSuperClassData.vehicleTable));
			assertNull(persistenceContext.getPersister(Vehicle.class));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void simpleCase_withTableDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)    // note : we don't need to embed Color because it is defined in the Dialect registry
					// concrete class defines id
					.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.add(Vehicle::getColor).getConfiguration())
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertFalse(tables.contains(mappedSuperClassData.vehicleTable));
			assertNull(persistenceContext.getPersister(Vehicle.class));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void columnNamingStrategyChanged() {
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)    // note : we don't need to embed Color because it is defined in the Dialect registry
					.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
					// concrete class defines id
					.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.add(Vehicle::getColor).getConfiguration())
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery("select id_col, model, color from Car", Car.class)
					.mapKey(Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("color", Car::setColor)
					.execute(persistenceContext.getConnectionProvider());
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void columnNamingStrategyChanged_inBoth() {
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)    // note : we don't need to embed Color because it is defined in the Dialect registry
					.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
					// concrete class defines id
					.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_superCol")
							.add(Vehicle::getColor).getConfiguration())
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery("select id_col, model, color_superCol from Car", Car.class)
					.mapKey(Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("color_superCol", Car::setColor)
					.execute(persistenceContext.getConnectionProvider());
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void withoutIdDefined_throwsException() {
			UnsupportedOperationException thrownException = assertThrows(UnsupportedOperationException.class,
					() -> FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
							.add(Car::getModel)
							.add(Car::getColor)
							.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
									.from(Vehicle.class)
									.add(Vehicle::getColor).getConfiguration())
							.build(persistenceContext));
			
			assertEquals("Identifier is not defined for o.g.s.p.e.FluentEntityMappingConfigurationSupportInheritanceTest$Car," 
							+ " please add one throught o.g.s.p.e.ColumnOptions.identifier(o.g.s.p.e.ColumnOptions$IdentifierPolicy)",
					thrownException.getMessage());
		}
		
		@Test
		void withEmbeddable() {
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					// concrete class defines id
					.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.embed(Vehicle::getColor).getConfiguration())
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			List<Car> allCars = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey(Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new)
					.execute(persistenceContext.getConnectionProvider());
			assertEquals(Arrays.asList(dummyCar), allCars);
		}
	}
	
	@Nested
	class Inheritance {
		
		@Test
		void withIdDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)	// note : we don't need to embed Color because it is defined in the Dialect registry
					.mapInheritance(inheritanceConfiguration)
					.build(persistenceContext);
			
			// as an inherited entity of non joined_tables policy, the table should not be in the context, but its persister does exist
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertFalse(tables.contains(mappedSuperClassData.vehicleTable));
			assertNotNull(persistenceContext.getPersister(Vehicle.class));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void identifierIsRedefined_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(AbstractVehicle.class, LONG_TYPE)
					.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED).getConfiguration();
			
			MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class,
					() -> FluentEntityMappingConfigurationSupport
					.from(Vehicle.class, LONG_TYPE)
					.mapInheritance(inheritanceConfiguration)
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(persistenceContext));
			assertEquals("Defining an identifier while inheritance is used is not supported", thrownException.getMessage());
		}
		
		@Test
		void multipleInheritance() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(AbstractVehicle.class, LONG_TYPE)
					.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration2 = FluentEntityMappingConfigurationSupport
					.from(Vehicle.class, LONG_TYPE)
					.mapInheritance(inheritanceConfiguration)
					.getConfiguration();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport
					.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)
					.mapInheritance(inheritanceConfiguration2)
					.build(persistenceContext);
			
			// as an inherited entity, the table should be in the context, and its persister does exist
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertFalse(tables.contains(mappedSuperClassData.vehicleTable));
			assertNotNull(persistenceContext.getPersister(Vehicle.class));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, mappedSuperClassData.carTable.idColumn, m -> m
					.add(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.add(mappedSuperClassData.carTable.colorColumn, Car::setColor));
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void mappedSuperClass_and_entityInheritance_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.getConfiguration();
			
			IFluentMappingBuilder<Car, Identifier<Long>> mappingBuilder = FluentEntityMappingConfigurationSupport.from(Car.class
					, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor)
					.mapSuperClass(FluentEmbeddableMappingConfigurationSupport
							.from(Vehicle.class)
							.add(Vehicle::getColor).getConfiguration())
					.mapInheritance(inheritanceConfiguration);
			MappingConfigurationException thrownException = assertThrows(MappingConfigurationException.class,
					() -> mappingBuilder.build(persistenceContext));
			assertEquals("Mapped super class and inheritance are not supported when they are combined, please remove one of them", thrownException.getMessage());
		}
		
		@Test
		void joinedTables() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Vehicle::getColor)
					.getConfiguration();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.mapInheritance(inheritanceConfiguration)
						.withJoinTable()
					.build(persistenceContext, mappedSuperClassData.carTable);
			
			// as an inherited entity, the table should be in the context
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertTrue(Iterables.collectToList(tables, Table::getName).contains(Vehicle.class.getSimpleName()));
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery(QueryEase.select(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.carTable.modelColumn, mappedSuperClassData.vehicleTable.colorColumn)
					.from(mappedSuperClassData.carTable).innerJoin(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.vehicleTable.idColumn), Car.class)
					.mapKey(Car::new, mappedSuperClassData.carTable.idColumn)
					.map(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.map(mappedSuperClassData.vehicleTable.colorColumn, Car::setColor)
					.execute(connectionProvider);
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
		@Test
		void withEmbeddable() {
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
					.from(Vehicle.class, LONG_TYPE)
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.embed(Vehicle::getColor)
					.getConfiguration();
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.mapInheritance(inheritanceConfiguration)
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			List<Car> allCars = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey(Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new)
					.execute(persistenceContext.getConnectionProvider());
			assertEquals(Arrays.asList(dummyCar), allCars);
		}
	}
	
	/**
	 * Data and method for common tests of building a strategy from a mapped super class
	 */
	static class MappedSuperClassData {
		
		abstract class AbstractVehiculeTable<SELF extends AbstractVehiculeTable<SELF>> extends Table<SELF> {
			final Column<SELF, Identifier<Long>> idColumn = addColumn("id", (Class<Identifier<Long>>) (Class) Identifier.class).primaryKey();
			final Column<SELF, Color> colorColumn = addColumn("color", Color.class);
			
			public AbstractVehiculeTable(String name) {
				super(name);
			}
		}
		
		class VehiculeTable extends AbstractVehiculeTable<VehiculeTable> {
			
			public VehiculeTable(String name) {
				super(name);
			}
		}
		
		class CarTable extends AbstractVehiculeTable<CarTable> {
			final Column<CarTable, String> modelColumn = addColumn("model", String.class);
			
			public CarTable(String name) {
				super(name);
			}
		}
		
		private final VehiculeTable vehicleTable = new VehiculeTable("vehicle");
		
		private final CarTable carTable = new CarTable("car");
	}
	
	static abstract class AbstractVehicle implements Identified<Long> {
		
		private Identifier<Long> id;
		
		private Timestamp timestamp;
		
		public AbstractVehicle() {
		}
		
		private AbstractVehicle(Identifier<Long> id) {
			this.id = id;
		}
		
		@Override
		public Identifier<Long> getId() {
			return id;
		}
		
		public Timestamp getTimestamp() {
			return timestamp;
		}
		
		public void setTimestamp(Timestamp timestamp) {
			this.timestamp = timestamp;
		}
	}
	
	static class Vehicle extends AbstractVehicle {
		
		private Color color;
		
		private Engine engine;
		
		public Vehicle(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		public Vehicle(Identifier<Long> id) {
			super(id);
		}
		
		public Vehicle() {
		}
		
		public Color getColor() {
			return color;
		}
		
		public void setColor(Color color) {
			this.color = color;
		}
		
		public Engine getEngine() {
			return engine;
		}
		
		public void setEngine(Engine engine) {
			this.engine = engine;
		}
	}
	
	static class Color {
		
		private int rgb;
		
		public Color() {
		}
		
		public Color(int rgb) {
			this.rgb = rgb;
		}
		
		public int getRgb() {
			return rgb;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Color color = (Color) o;
			return rgb == color.rgb;
		}
		
		@Override
		public int hashCode() {
			return Objects.hash(rgb);
		}
	}
	
	static class Car extends Vehicle {
		
		private String model;
		
		public Car() {
		}
		
		public Car(Long id) {
			this(new PersistableIdentifier<>(id));
		}
		
		public Car(Identifier<Long> id) {
			super(id);
		}
		
		public Car(Long id, String model) {
			this(new PersistableIdentifier<>(id), model);
		}
		
		public Car(Identifier<Long> id, String model) {
			super(id);
			setModel(model);
		}
		
		public String getModel() {
			return model;
		}
		
		public void setModel(String model) {
			this.model = model;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Car car = (Car) o;
			return Objects.equals(model, car.model);
		}
		
		/**
		 * Implemented for easier debug
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return "Car{id=" + getId().getSurrogate() + ", color=" + getColor().getRgb() + ", model='" + model + "\'}";
		}
	}
	
	private static class Bicycle extends Vehicle {
		
		private Bicycle(Identifier<Long> id) {
			super(id);
		}
	}
	
	private static class Truk extends Vehicle {
		
		private Truk(Identifier<Long> id) {
			super(id);
		}
	}
	
	static class Engine implements Identified<Long> {
		
		private Identifier<Long> id;
		
		public Engine() {
		}
		
		public Engine(Long id) {
			this.id = new PersistableIdentifier<>(id);
		}
		
		public Engine(Identifier<Long> id) {
			this.id = id;
		}
		
		public Identifier<Long> getId() {
			return id;
		}
	}
}
