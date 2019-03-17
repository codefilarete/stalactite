package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.lang.collection.Arrays;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.LambdaParameterBinder;
import org.gama.sql.binder.NullAwareParameterBinder;
import org.gama.sql.test.HSQLDBInMemoryDataSource;
import org.gama.stalactite.persistence.engine.FluentMappingBuilder.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.IFluentMappingBuilder.IFluentMappingBuilderColumnOptions;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.HSQLDBDialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilderInheritanceTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
	private static IFluentMappingBuilderColumnOptions<City, Identifier<Long>> CITY_MAPPING_BUILDER;
	private final DataSource dataSource = new HSQLDBInMemoryDataSource();
	private final ConnectionProvider connectionProvider = new JdbcConnectionProvider(dataSource);
	private Persister<City, Identifier<Long>, ?> cityPersister;
	private PersistenceContext persistenceContext;
	
	@BeforeAll
	public static void initBinders() {
		// binder creation for our identifier
		DIALECT.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		DIALECT.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(LONG_PRIMITIVE_BINDER));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		DIALECT.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRGB)));
		DIALECT.getJavaTypeToSqlTypeMapping().put(Color.class, "int");
	}
	
	@BeforeEach
	public void beforeTest() {
		persistenceContext = new PersistenceContext(connectionProvider, DIALECT);
		
		// We need to rebuild our cityPersister before each test because some of them alter it on country relationship.
		// So schema contains FK twice with same name, ending in duplicate FK name exception
		CITY_MAPPING_BUILDER = FluentMappingBuilder.from(City.class,
				LONG_TYPE)
				.add(City::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
				.add(City::getName)
				.add(City::getCountry);
		cityPersister = CITY_MAPPING_BUILDER.build(persistenceContext);
	}
	
	/**
	 * Data and method for common tests of building a strategy from a mapped super class
	 */
	private class MappedSuperClassData {
		
		class VehiculeTable<SELF extends VehiculeTable<SELF>> extends Table<SELF> {
			final Column<SELF, Identifier<Long>> idColumn = addColumn("id", (Class<Identifier<Long>>) (Class) Identifier.class).primaryKey();
			final Column<SELF, Color> colorColumn = addColumn("color", Color.class);
			
			public VehiculeTable(String name) {
				super(name);
			}
		}
		
		class CarTable extends VehiculeTable<CarTable> {
			final Column<CarTable, String> modelColumn = addColumn("model", String.class);
			
			public CarTable(String name) {
				super(name);
			}
		}
		
		private final VehiculeTable vehicleTable = new VehiculeTable("vehicule");
		
		private final CarTable carTable = new CarTable("car");
		
		void executeTest(Consumer<IFluentMappingBuilder<Car, Identifier<Long>>> additionalConfigurator) {
			executeTest(additionalConfigurator, context ->
					context.select(Car::new, carTable.idColumn, m -> m
					.add(carTable.modelColumn, Car::setModel)
					.add(carTable.colorColumn, Car::setColor)));
		}
		
		void executeTest(Consumer<IFluentMappingBuilder<Car, Identifier<Long>>> additionalConfigurator, Function<PersistenceContext, List<Car>> persistedCarsSupplier) {
			
			IFluentMappingBuilder<Car, Identifier<Long>> carMappingBuilder = FluentMappingBuilder.from(Car.class, LONG_TYPE)
					.add(Car::getModel)
					.add(Car::getColor);	// note : we don't need to embed Color because it is defined in the Dialect registry
			
			additionalConfigurator.accept(carMappingBuilder);
			
			Persister<Car, Identifier<Long>, ?> carPersister = carMappingBuilder
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistedCarsSupplier.apply(persistenceContext);
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
		}
		
	}
	
	@Nested
	public class MappedSuperClass {
		
		@Test
		public void superClassIsEmbeddable_withIdDefinedInConcreteClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EmbeddedBeanMappingStrategy<Vehicle, Table> vehicleMappingStrategy = FluentEmbeddableMappingConfigurationSupport
					.from(Vehicle.class)
					.add(Vehicle::getColor)
					.build(DIALECT, mappedSuperClassData.vehicleTable);
			
			mappedSuperClassData.executeTest(carMappingBuilder ->
					carMappingBuilder
							// concrete class defines id
							.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.mapSuperClass(Vehicle.class, vehicleMappingStrategy));
		}
		
		@Test
		public void columnNamingStrategyChanged() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EmbeddedBeanMappingStrategy<Vehicle, Table> vehicleMappingStrategy = FluentEmbeddableMappingConfigurationSupport
					.from(Vehicle.class)
					.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_superCol")
					.add(Vehicle::getColor)
					.build(DIALECT, mappedSuperClassData.vehicleTable);
			
			mappedSuperClassData.executeTest(carMappingBuilder ->
					carMappingBuilder
							.columnNamingStrategy(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
							// concrete class defines id
							.add(Car::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
							.mapSuperClass(Vehicle.class, vehicleMappingStrategy),
					// NB: model column name is not taken into account because of its mapping precedence at runtime in this test
					// due to the "additionalConfigurator" way of doing
					context -> context.newQuery("select id_col, model, color_superCol from Car", Car.class)
							.mapKey(Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
							.map("model", Car::setModel)
							.map("color_superCol", Car::setColor)
							.execute(context.getConnectionProvider()));
		}
		
		@Test
		public void superClassIsEmbeddable_withoutIdDefined_throwsException() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EmbeddedBeanMappingStrategy<Vehicle, Table> vehicleMappingStrategy = FluentEmbeddableMappingConfigurationSupport
					.from(Vehicle.class)
					.add(Vehicle::getColor)
					.build(DIALECT, mappedSuperClassData.vehicleTable);
			
			UnsupportedOperationException thrownException = assertThrows(UnsupportedOperationException.class,
					() -> mappedSuperClassData.executeTest(carMappingBuilder ->
							carMappingBuilder
									.mapSuperClass(Vehicle.class, vehicleMappingStrategy)));
			
			assertEquals("Identifier is not defined,"
							+ " please add one throught o.g.s.p.e.IFluentMappingBuilder o.g.s.p.e.ColumnOptions.identifier(o.g.s.p.e.FluentMappingBuilder$IdentifierPolicy)",
					thrownException.getMessage());
		}
		
		@Test
		public void superClassIsEntity_withIdDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			ClassMappingStrategy<Vehicle, Identifier<Long>, Table> vehicleMappingStrategy = FluentMappingBuilder
					.from(Vehicle.class, LONG_TYPE, mappedSuperClassData.vehicleTable)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(DIALECT);
			
			mappedSuperClassData.executeTest(carMappingBuilder ->
					carMappingBuilder
							.mapSuperClass(Vehicle.class, vehicleMappingStrategy));
		}
		
		@Test
		public void superClassIsEntity_multipleInheritance() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			ClassMappingStrategy<AbstractVehicle, Identifier<Long>, Table> abstractVehicleMappingStrategy = FluentMappingBuilder
					.from(AbstractVehicle.class, LONG_TYPE, mappedSuperClassData.vehicleTable)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(DIALECT);
			
			ClassMappingStrategy<Vehicle, Identifier<Long>, Table> vehicleMappingStrategy = FluentMappingBuilder
					.from(Vehicle.class, LONG_TYPE, mappedSuperClassData.vehicleTable)
					.mapSuperClass(AbstractVehicle.class, abstractVehicleMappingStrategy)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(DIALECT);
			
			mappedSuperClassData.executeTest(carMappingBuilder ->
					carMappingBuilder
							.mapSuperClass(Vehicle.class, vehicleMappingStrategy));
		}
		
		@Test
		public void superClassIsEntity_multipleMappedSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			ClassMappingStrategy<AbstractVehicle, Identifier<Long>, Table> abstractVehicleMappingStrategy = FluentMappingBuilder
					.from(AbstractVehicle.class, LONG_TYPE, mappedSuperClassData.vehicleTable)
					// mapped super class defines id
					.add(AbstractVehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(DIALECT);
			
			ClassMappingStrategy<Vehicle, Identifier<Long>, Table> vehicleMappingStrategy = FluentMappingBuilder
					.from(Vehicle.class, LONG_TYPE, mappedSuperClassData.vehicleTable)
					// mapped super class defines id
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.build(DIALECT);
			
			mappedSuperClassData.executeTest(carMappingBuilder ->
					carMappingBuilder
							.mapSuperClass(Vehicle.class, vehicleMappingStrategy)
							.mapSuperClass(AbstractVehicle.class, abstractVehicleMappingStrategy)
			);
		}
	}
	
	// TODO: à tester
	// - la table de la classe parente ne doit pas exister
	// - les embeddables de la classe parente
	// - le shadowing
	// - tester les foreigns key entrantes
	// - différent type de mapping : TablePerClass, SingleTable, JoinedTable
	// - TablePerClass: 1 table créée par entité, select sans jointure sauf avec les relations de l'entité
	// - SingleTable : un table monolithique, aucune colonne obligatoire (sauf PK), ne lire que les colonnes de l'entité (et sous entité) et tronc commun
	// - JoinedTable : 1 table créé par entité + tron commun, select avec jointure entre ces tables
	
	
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
		
		private Vehicle(Identifier<Long> id) {
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
	}
	
	static class Color {
		
		private final int rgb;
		
		Color(int rgb) {
			this.rgb = rgb;
		}
		
		public int getRGB() {
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
			return "Car{id=" + getId().getSurrogate() + ", color=" + getColor().getRGB() + ", model='" + model + "\'}";
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
	
	public static class VehicleRecord {
		
		private final long id;
		
		private final String color;
		
		public VehicleRecord(long id, String color) {
			this.id = id;
			this.color = color;
		}
		
		public Long getId() {
			return id;
		}
		
		/**
		 * Implemented for easier debug
		 * @return a simple representation of this
		 */
		@Override
		public String toString() {
			return "VehicleRecord{id=" + id + ", color='" + color + '\'' + '}';
		}
	}
}
