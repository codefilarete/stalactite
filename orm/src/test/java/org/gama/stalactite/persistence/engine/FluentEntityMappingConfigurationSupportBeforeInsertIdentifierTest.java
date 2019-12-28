package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.function.Sequence;
import org.gama.lang.trace.ModifiableInt;
import org.gama.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.lang.Nullable.nullable;
import static org.gama.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportBeforeInsertIdentifierTest {
	
	private HSQLDBDialect dialect = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	private Sequence<Long> longSequence;
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), dialect);
		longSequence = new Sequence<Long>() {
			
			private final ModifiableInt counter = new ModifiableInt(0);
			
			@Override
			public Long next() {
				return (long) counter.increment();
			}
		};
	}
	
	@Test
	void insert_basic() {
		IPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.add(Car::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
				.add(Car::getModel)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		List<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey(Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertEquals(Arrays.asList(dummyCar), allCars);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertEquals(dummyCar, loadedCar);
	}
	
	@Test
	void insert_oneToOne() {
		IPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.add(Car::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
				.add(Car::getModel)
				.addOneToOne(Car::getEngine, entityBuilder(Engine.class, long.class)
					.add(Engine::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
					.add(Engine::getModel))
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		dummyCar.setEngine(new Engine("XFE45K-TRE"));
		
		// insert test
		carPersister.insert(dummyCar);
		assertNotNull(dummyCar.getEngine().getId());
		
		List<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey(Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertEquals(Arrays.asList(dummyCar), allCars);
		
		// select test
		Car loadedCar = carPersister.select(2L);	// 2 because Engin was inserted first and sequence is shared between Car and Engine (for tests purpose)
		assertEquals(dummyCar, loadedCar);
		assertEquals(dummyCar.getEngine(), loadedCar.getEngine());
	}
	
	@Test
	void insert_oneToOne_ownedByReverseSide() {
		IPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.add(Car::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
				.add(Car::getModel)
				.addOneToOne(Car::getEngine, entityBuilder(Engine.class, long.class)
					.add(Engine::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
					.add(Engine::getModel))
				.mappedBy(Engine::getCar)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		Engine engine = new Engine("XFE45K-TRE");
		dummyCar.setEngine(engine);
		engine.setCar(dummyCar);
		
		// insert test
		carPersister.insert(dummyCar);
		assertNotNull(dummyCar.getEngine().getId());
		
		List<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey(Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertEquals(Arrays.asList(dummyCar), allCars);
		
		// select test
		Car loadedCar = carPersister.select(1L);	// 1 because Car was inserted first because it owns the property
		assertEquals(dummyCar, loadedCar);
		assertEquals(dummyCar.getEngine(), loadedCar.getEngine());
	}
	
	@Test
	void multipleInheritance() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
				.getConfiguration();
		
		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = entityBuilder(Vehicle.class, long.class)
				.mapInheritance(inheritanceConfiguration)
				.getConfiguration();
		
		IPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.add(Car::getModel)
				.mapInheritance(inheritanceConfiguration2)
				.build(persistenceContext);
		
		assertEquals("Car", persistenceContext.getPersister(Car.class).getMappingStrategy().getTargetTable().getName());
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		List<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey(Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertEquals(Arrays.asList(dummyCar), allCars);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertEquals(dummyCar, loadedCar);
	}
	
	@Test
	void multipleInheritance_joinedTables() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.beforeInsert(longSequence))
				.getConfiguration();
		
		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = entityBuilder(Vehicle.class, long.class)
				.mapInheritance(inheritanceConfiguration).withJoinedTable()
				.getConfiguration();
		
		IPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.add(Car::getModel)
				.mapInheritance(inheritanceConfiguration2).withJoinedTable()
				.build(persistenceContext);
		
		assertEquals(Arrays.asHashSet("Car", "Vehicle", "AbstractVehicle"),
				DDLDeployer.collectTables(persistenceContext).stream().map(Table::getName).collect(Collectors.toSet()));
		assertEquals("Car", persistenceContext.getPersister(Car.class).getMappingStrategy().getTargetTable().getName());
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car(1L);
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		List<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey(Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertEquals(Arrays.asList(dummyCar), allCars);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertEquals(dummyCar, loadedCar);
	}
	
	static abstract class AbstractVehicle {
		
		private Long id;
		
		private Timestamp timestamp;
		
		public AbstractVehicle() {
		}
		
		private AbstractVehicle(Long id) {
			this.id = id;
		}
		
		public Long getId() {
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
			super(id);
		}
		
		public Car(Long id, String model) {
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
			return "Car{id=" + getId() + ", color=" + nullable(getColor()).map(Color::getRgb).get() + ", model='" + model + "\'}";
		}
	}
	
	static class Engine {
		
		private Long id;
		
		private String model;
		
		private Car car;
		
		public Engine() {
		}
		
		public Engine(String model) {
			this.model = model;
		}
		
		public Long getId() {
			return id;
		}
		
		public String getModel() {
			return model;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Engine engine = (Engine) o;
			return Objects.equals(id, engine.id);
		}
		
		@Override
		public int hashCode() {
			return id != null ? id.hashCode() : 0;
		}
		
		/**
		 * Implemented for easier debug
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return "Engine{id=" + id + '}';
		}
		
		public Car getCar() {
			return car;
		}
		
		public void setCar(Car car) {
			this.car = car;
		}
	}
}
