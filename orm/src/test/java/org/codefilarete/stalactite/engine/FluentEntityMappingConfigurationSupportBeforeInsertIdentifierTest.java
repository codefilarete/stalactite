package org.codefilarete.stalactite.engine;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer;
import org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.function.Sequence;
import org.codefilarete.tool.trace.ModifiableInt;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.STRING_READER;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportBeforeInsertIdentifierTest {
	
	private Dialect dialect = new HSQLDBDialect();
	private DataSource dataSource = new HSQLDBInMemoryDataSource();
	private PersistenceContext persistenceContext;
	private Sequence<Long> longSequence;
	
	@BeforeEach
	public void initTest() {
		persistenceContext = new PersistenceContext(dataSource, dialect);
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
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
				.map(Car::getModel)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute(Accumulators.toSet());
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertThat(loadedCar).isEqualTo(dummyCar);
	}
	
	@Test
	void insert_basic_withDatabaseSequence() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.databaseSequence("CAR_SEQUENCE"))
				.map(Car::getModel)
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute(Accumulators.toSet());
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertThat(loadedCar).isEqualTo(dummyCar);
	}
	
	@Test
	void insert_oneToOne() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
				.map(Car::getModel)
				.mapOneToOne(Car::getEngine, entityBuilder(Engine.class, long.class)
						.mapKey(Engine::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
						.map(Engine::getModel))
				.build(persistenceContext);
		
		// DML tests
		DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
		ddlDeployer.deployDDL();
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		dummyCar.setEngine(new Engine("XFE45K-TRE"));
		
		// insert test
		carPersister.insert(dummyCar);
		assertThat(dummyCar.getEngine().getId()).isNotNull();
		
		ResultSetRowTransformer<Engine, Long> engineTransformer = new ResultSetRowTransformer<>(Engine.class, "engineId", DefaultResultSetReaders.LONG_READER, Engine::new);
		engineTransformer.add("engineModel", STRING_READER, Engine::setModel);
		BeanRelationFixer<Car, Engine> carEngineRelationFixer = BeanRelationFixer.of(Car::setEngine);
		Set<Car> allCars = persistenceContext.newQuery("select id, model, Engine.id as engineId, Engine.model as engineModel from Car inner join Engine on Car.engineId = Engine.id", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.map(carEngineRelationFixer, engineTransformer)
				.execute(Accumulators.toSet());
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(2L);	// 2 because Engin was inserted first and sequence is shared between Car and Engine (for tests purpose)
		assertThat(loadedCar).isEqualTo(dummyCar);
		assertThat(loadedCar.getEngine()).isEqualTo(dummyCar.getEngine());
	}
	
	@Test
	void insert_oneToOne_ownedByReverseSide() {
		EntityPersister<Car, Long> carPersister = entityBuilder(Car.class, long.class)
				.mapKey(Car::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
				.map(Car::getModel)
				.mapOneToOne(Car::getEngine, entityBuilder(Engine.class, long.class)
						.mapKey(Engine::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
						.map(Engine::getModel))
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
		assertThat(dummyCar.getEngine().getId()).isNotNull();
		
		ResultSetRowTransformer<Engine, Long> engineTransformer = new ResultSetRowTransformer<>(Engine.class, "engineId", DefaultResultSetReaders.LONG_READER, Engine::new);
		engineTransformer.add("engineModel", STRING_READER, Engine::setModel);
		BeanRelationFixer<Car, Engine> carEngineRelationFixer = BeanRelationFixer.of(Car::setEngine);
		Set<Car> allCars = persistenceContext.newQuery("select id, model, Engine.id as engineId, Engine.model as engineModel from Car inner join Engine on Car.id = Engine.carId", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.map(carEngineRelationFixer, engineTransformer)
				.execute(Accumulators.toSet());
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(1L);	// 1 because Car was inserted first because it owns the property
		assertThat(loadedCar).isEqualTo(dummyCar);
		assertThat(loadedCar.getEngine()).isEqualTo(dummyCar.getEngine());
	}
	
	@Test
	void multipleInheritance() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
				.getConfiguration();
		
		EntityMappingConfiguration<Vehicle, Long> inheritanceConfiguration2 = entityBuilder(Vehicle.class, long.class)
				.map(Vehicle::getName)
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
		
		Car dummyCar = new Car();
		dummyCar.setName("Toto");
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model, name from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.map("name", Car::setName)
				.execute(Accumulators.toSet());
		assertThat(allCars)
				.containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertThat(loadedCar).isEqualTo(dummyCar);
	}
	
	@Test
	void multipleInheritance_joinedTables() {
		EntityMappingConfiguration<AbstractVehicle, Long> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, long.class)
				.mapKey(AbstractVehicle::getId, IdentifierPolicy.pooledHiLoSequence(longSequence))
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
		
		Car dummyCar = new Car();
		dummyCar.setModel("Renault");
		
		// insert test
		carPersister.insert(dummyCar);
		
		Set<Car> allCars = persistenceContext.newQuery("select id, model from Car", Car.class)
				.mapKey((SerializableFunction<Long, Car>) Car::new, "id", long.class)
				.map("model", Car::setModel)
				.execute(Accumulators.toSet());
		assertThat(allCars).containsExactlyInAnyOrder(dummyCar);
		
		// select test
		Car loadedCar = carPersister.select(1L);
		assertThat(loadedCar).isEqualTo(dummyCar);
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
		
		private String name;
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
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
			return EqualsBuilder.reflectionEquals(this, o);
		}
		
		/**
		 * Implemented for easier debug
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}
	}
	
	static class Engine {
		
		private Long id;
		
		private String model;
		
		private Car car;
		
		public Engine() {
		}
		
		public Engine(Long id) {
			this.id = id;
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
		
		public void setModel(String model) {
			this.model = model;
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
