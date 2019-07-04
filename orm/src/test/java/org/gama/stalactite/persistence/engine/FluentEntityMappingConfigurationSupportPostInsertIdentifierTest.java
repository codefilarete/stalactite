package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupportInheritanceTest.Engine;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.test.JdbcConnectionProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.gama.lang.Nullable.nullable;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportPostInsertIdentifierTest {
	
	private HSQLDBDialect dialect = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(new JdbcConnectionProvider(dataSource), dialect);
	}
	
	@Test
	void insert_basic() {
		Persister<Car, Long, ?> carPersister = FluentEntityMappingConfigurationSupport.from(Car.class, long.class)
				.add(Car::getId).identifier(IdentifierPolicy.AFTER_INSERT)
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
	void multipleInheritance() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
				.from(AbstractVehicle.class, long.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = FluentEntityMappingConfigurationSupport
				.from(Vehicle.class, long.class)
				.mapInheritance(inheritanceConfiguration)
				.getConfiguration();
		
		Persister<Car, Long, ?> carPersister = FluentEntityMappingConfigurationSupport
				.from(Car.class, long.class)
				.add(Car::getModel)
				.mapInheritance(inheritanceConfiguration2)
				.build(persistenceContext);
		
		assertEquals("Car", persistenceContext.getPersister(Vehicle.class).getMainTable().getName());
		
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
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = FluentEntityMappingConfigurationSupport
				.from(AbstractVehicle.class, long.class)
				.add(AbstractVehicle::getId).identifier(IdentifierPolicy.AFTER_INSERT)
				.getConfiguration();
		
		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = FluentEntityMappingConfigurationSupport
				.from(Vehicle.class, long.class)
				.mapInheritance(inheritanceConfiguration).withJoinTable()
				.getConfiguration();
		
		Persister<Car, Long, ?> carPersister = FluentEntityMappingConfigurationSupport
				.from(Car.class, long.class)
				.add(Car::getModel)
				.mapInheritance(inheritanceConfiguration2).withJoinTable()
				.build(persistenceContext);
		
		assertEquals(Arrays.asHashSet("Car", "Vehicle", "AbstractVehicle"),
				DDLDeployer.collectTables(persistenceContext).stream().map(Table::getName).collect(Collectors.toSet()));
		assertEquals("AbstractVehicle", persistenceContext.getPersister(AbstractVehicle.class).getMainTable().getName());
		assertEquals("Vehicle", persistenceContext.getPersister(Vehicle.class).getMainTable().getName());
		assertEquals("Car", persistenceContext.getPersister(Car.class).getMainTable().getName());
		
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
			return "Car{id=" + getId() + ", color=" + nullable(getColor()).orGet(Color::getRgb) + ", model='" + model + "\'}";
		}
	}
}
