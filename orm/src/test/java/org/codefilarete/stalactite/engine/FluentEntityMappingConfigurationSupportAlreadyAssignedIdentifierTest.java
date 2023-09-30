package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.collection.Arrays;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportAlreadyAssignedIdentifierTest {
	
	private HSQLDBDialect dialect = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(dataSource, dialect);
	}
	
	@Test
	void insert_basic() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.alreadyAssigned(Car::markAsPersisted, Car::isPersisted))
				.map(Car::getModel)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car(42L);
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		assertThat(dummyCar.isPersisted()).as("car is persisted").isTrue();
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(42L);
		assertThat(loadedCar).isEqualTo(dummyCar);
		assertThat(loadedCar.isPersisted()).as("loaded car is mark as persisted").isTrue();
	}
	
	@Test
	void insert_oneToOne() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.alreadyAssigned(Car::markAsPersisted, Car::isPersisted))
				.map(Car::getModel)
				.mapOneToOne(Car::getEngine, entityBuilder(Engine.class, String.class)
						.mapKey(Engine::getModel, IdentifierPolicy.alreadyAssigned(Engine::markAsPersisted, Engine::isPersisted))
					)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car(42L);
		dummyCar.setModel("Renault");
		dummyCar.setEngine(new Engine("XFE45K-TRE"));
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(42L);
		assertThat(loadedCar).isEqualTo(dummyCar);
		assertThat(loadedCar.getEngine()).isEqualTo(dummyCar.getEngine());
	}

	@Test
	void insert_oneToOne_ownedByReverseSide() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.alreadyAssigned(Car::markAsPersisted, Car::isPersisted))
				.map(Car::getModel)
				.mapOneToOne(Car::getEngine, entityBuilder(Engine.class, String.class)
						.mapKey(Engine::getModel, IdentifierPolicy.alreadyAssigned(Engine::markAsPersisted, Engine::isPersisted))
					)
				.mappedBy(Engine::getCar)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car(42L);
		dummyCar.setModel("Renault");
		Engine engine = new Engine("XFE45K-TRE");
		dummyCar.setEngine(engine);
		engine.setCar(dummyCar);
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(42L);
		assertThat(loadedCar).isEqualTo(dummyCar);
		assertThat(loadedCar.getEngine()).isEqualTo(dummyCar.getEngine());
	}

	@Test
	void multipleInheritance() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.alreadyAssigned(AbstractVehicle::markAsPersisted, AbstractVehicle::isPersisted))
				.getConfiguration();

		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = entityBuilder(Vehicle.class, long.class)
				.mapSuperClass(inheritanceConfiguration)
				.getConfiguration();

		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.map(Car::getModel)
				.mapSuperClass(inheritanceConfiguration2)
				.build(persistenceContext);

		assertThat(((ConfiguredPersister) persistenceContext.getPersister(Car.class)).getMapping().getTargetTable().getName()).isEqualTo(
				"Car");

		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Car dummyCar = new Car(42L);
		dummyCar.setModel("Renault");

		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);

		// select test
		Car loadedCar = carPersister.select(42L);
		assertThat(loadedCar).isEqualTo(dummyCar);
	}

	@Test
	void multipleInheritance_joinedTables() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.alreadyAssigned(AbstractVehicle::markAsPersisted, AbstractVehicle::isPersisted))
				.getConfiguration();

		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = entityBuilder(Vehicle.class, long.class)
				.mapSuperClass(inheritanceConfiguration).withJoinedTable()
				.getConfiguration();

		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.map(Car::getModel)
				.mapSuperClass(inheritanceConfiguration2).withJoinedTable()
				.build(persistenceContext);

		assertThat(DDLDeployer.collectTables(persistenceContext).stream().map(Table::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asHashSet("Car", "Vehicle", "AbstractVehicle"));
		assertThat(((ConfiguredPersister) persistenceContext.getPersister(Car.class)).getMapping().getTargetTable().getName()).isEqualTo("Car");

		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();

		Car dummyCar = new Car(42L);
		dummyCar.setModel("Renault");

		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute();
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);

		// select test
		Car loadedCar = carPersister.select(42L);
		assertThat(loadedCar).isEqualTo(dummyCar);
	}

	static abstract class AbstractVehicle {

		private Long id;

		private Timestamp timestamp;
		
		private boolean persisted = false;

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
		
		public boolean isPersisted() {
			return persisted;
		}
		
		public void markAsPersisted() {
			this.persisted = true;
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

		private boolean persisted = false;

		private String model;

		private Car car;

		public Engine() {
		}

		public Engine(String model) {
			this.model = model;
		}

		public boolean isPersisted() {
			return persisted;
		}
		
		public void markAsPersisted() {
			this.persisted = true;
		}
		
		public String getModel() {
			return model;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Engine engine = (Engine) o;
			return Objects.equals(model, engine.model);
		}

		@Override
		public int hashCode() {
			return model != null ? model.hashCode() : 0;
		}

		/**
		 * Implemented for easier debug
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return "Engine(" + Strings.footPrint(this, Engine::getModel) + ")";
		}

		public Car getCar() {
			return car;
		}

		public void setCar(Car car) {
			this.car = car;
		}
	}
}
