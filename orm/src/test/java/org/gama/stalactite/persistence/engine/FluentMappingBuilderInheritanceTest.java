package org.gama.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.List;
import java.util.Objects;

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
	
	@Nested
	public class MappedSuperClass {
		
		@Test
		public void testInsert() {
			class VehiculeTable<SELF extends VehiculeTable<SELF>> extends Table<SELF> {
				final Column<SELF, Identifier<Long>> idColumn = addColumn("id", (Class<Identifier<Long>>) (Class) Identifier.class).primaryKey();
				final Column<SELF, Color> colorColumn = addColumn("color", Color.class);
				
				public VehiculeTable(String name) {
					super(name);
				}
			}
			
			VehiculeTable vehicleTable = new VehiculeTable("vehicule");
			
			class CarTable extends VehiculeTable<CarTable> {
				final Column<CarTable, String> modelColumn = addColumn("model", String.class);
				
				public CarTable(String name) {
					super(name);
				}
			}
			
			CarTable carTable = new CarTable("car");
			
			EmbeddedBeanMappingStrategy<Vehicle, Table> vehicleMappingStrategy = FluentMappingBuilder.from(Vehicle.class, LONG_TYPE, vehicleTable)
					.add(Vehicle::getColor)
					.buildEmbeddable(DIALECT);
			
			Persister<Car, Identifier<Long>, ?> carPersister = FluentMappingBuilder.from(Car.class, LONG_TYPE)
					.add(Vehicle::getId).identifier(IdentifierPolicy.ALREADY_ASSIGNED)
					.add(Car::getModel)
					.mapSuperClass(Vehicle.class, vehicleMappingStrategy)
					.embed(Car::getColor)
					.build(persistenceContext);
			
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.select(Car::new, carTable.idColumn, m -> m
					.add(carTable.modelColumn, Car::setModel)
					.add(carTable.colorColumn, Car::setColor));
			assertEquals(Arrays.asList(dummyCar), allCars);
			
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertEquals(dummyCar, loadedCar);
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
