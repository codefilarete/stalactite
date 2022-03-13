package org.codefilarete.stalactite.persistence.engine;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.persistence.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.persistence.engine.model.Car;
import org.codefilarete.stalactite.persistence.engine.model.Color;
import org.codefilarete.stalactite.persistence.engine.model.Vehicle;
import org.codefilarete.stalactite.persistence.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.persistence.id.Identifier;
import org.codefilarete.stalactite.persistence.id.PersistedIdentifier;
import org.codefilarete.stalactite.persistence.sql.HSQLDBDialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.CurrentThreadConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.codefilarete.stalactite.persistence.engine.MappingEase.embeddableBuilder;
import static org.codefilarete.stalactite.persistence.engine.MappingEase.entityBuilder;
import static org.codefilarete.stalactite.persistence.id.Identifier.LONG_TYPE;
import static org.codefilarete.stalactite.persistence.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
import static org.codefilarete.stalactite.query.model.QueryEase.select;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.LONG_PRIMITIVE_BINDER;

/**
 * @author Guillaume Mary
 */
public class FluentEntityMappingConfigurationSupportInheritanceTest {
	
	private static final HSQLDBDialect DIALECT = new HSQLDBDialect();
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
	
	@Nested
	class MappedSuperClass {
		
		@Test
		void simpleCase() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(persistenceContext.getPersister(Vehicle.class)).isNull();
			
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
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void simpleCase_withTableDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// as a mapped super class, the table shouldn't be in the context, nor its persister exists
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(persistenceContext.getPersister(Vehicle.class)).isNull();
			
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
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.withColumnNaming(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col")
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery("select id_col, model_col, color_col from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_col", Car::setModel)
					.map("color_col", Car::setColor)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged_inParent() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase
							.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor)
							.withColumnNaming(accessor -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_supercol"))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery("select id_supercol, model_supercol, color_supercol from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_supercol", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_supercol", Car::setModel)
					.map("color_supercol", Car::setColor)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void columnNamingStrategyChanged_inBoth_lowestTakesPriority() {
			ColumnNamingStrategy columnNamingStrategy = accessor -> {
				if (accessor.getName().contains("Color")) {
					return ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_superCol";
				} else {
					return ColumnNamingStrategy.DEFAULT.giveName(accessor) + "_col";
				}
			};
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.withColumnNaming(columnNamingStrategy)
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase
							.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.build(persistenceContext);
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery("select id_col, model_col, color_col from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id_col", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model_col", Car::setModel)
					.map("color_col", Car::setColor)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void withoutIdDefined_throwsException() {
			assertThatThrownBy(() -> entityBuilder(Car.class, LONG_TYPE)
						.map(Car::getModel)
						.map(Car::getColor)
						.mapSuperClass(MappingEase
								.embeddableBuilder(Vehicle.class)
								.map(Vehicle::getColor))
						.build(persistenceContext))
					.isInstanceOf(UnsupportedOperationException.class)
					.hasMessage("Identifier is not defined for o.c.s.p.e.m.Car,"
							+ " please add one through o.c.s.p.e.FluentEntityMappingBuilder.mapKey(o.d.j.u.f.s.SerializableBiConsumer, o.c.s.p.e.ColumnOptions$IdentifierPolicy)");
		}
		
		@Test
		void withEmbeddable() {
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					// concrete class defines id
					.mapKey(Car::getId, ALREADY_ASSIGNED)
					.mapSuperClass(MappingEase
							.embeddableBuilder(Vehicle.class)
							.embed(Vehicle::getColor, embeddableBuilder(Color.class)
									.map(Color::getRgb)))
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
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			List<Car> allCars = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
		}
	}
	
	@Nested
	class Inheritance {
		
		@Test
		void withIdDefinedInSuperClass() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)	// note : we don't need to embed Color because it is defined in the Dialect registry
					.mapInheritance(inheritanceConfiguration)
					.build(persistenceContext);
			
			// as an inherited entity of non joined_tables policy, the table should not be in the context, but its persister does exist
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(persistenceContext.getPersister(Vehicle.class)).isNotNull();
			
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
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void identifierIsRedefined_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED).getConfiguration();
			
			assertThatThrownBy(() -> entityBuilder(Vehicle.class, LONG_TYPE)
							.mapInheritance(inheritanceConfiguration)
							.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
							.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Defining an identifier while inheritance is used is not supported"
							+ " : o.c.s.p.e.m.Vehicle defines identifier AbstractVehicle::getId"
							+ " while it inherits from o.c.s.p.e.m.AbstractVehicle");
		}
		
		@Test
		void multipleInheritance() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration2 = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapInheritance(inheritanceConfiguration)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)
					.mapInheritance(inheritanceConfiguration2)
					.build(persistenceContext);
			
			// as an inherited entity, the table should not be in the context, and its persister does exist
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(tables.contains(mappedSuperClassData.vehicleTable)).isFalse();
			assertThat(tables.contains(mappedSuperClassData.carTable)).isTrue();
			assertThat(persistenceContext.getPersister(Vehicle.class)).isNotNull();
			
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
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void multipleInheritance_joinedTables() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
					.getConfiguration();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration2 = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapInheritance(inheritanceConfiguration).withJoinedTable()
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)
					.mapInheritance(inheritanceConfiguration2).withJoinedTable()
					.build(persistenceContext);
			
			// as an inherited entity, the table should be in the context, and its persister does exist
			assertThat(DDLDeployer.collectTables(persistenceContext).stream().map(Table::getName).collect(Collectors.toSet())).isEqualTo(Arrays.asHashSet("Car", "Vehicle", "AbstractVehicle"));
			assertThat(((EntityConfiguredPersister) persistenceContext.getPersister(Car.class)).getMappingStrategy().getTargetTable().getName()).isEqualTo("Car");
			
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
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void mappedSuperClass_and_entityInheritance_throwsException() {
			EntityMappingConfiguration<AbstractVehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(AbstractVehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(AbstractVehicle::getId, ALREADY_ASSIGNED)
					.getConfiguration();
			
			FluentEntityMappingBuilder<Car, Identifier<Long>> mappingBuilder = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.map(Car::getColor)
					.mapSuperClass(MappingEase.embeddableBuilder(Vehicle.class)
							.map(Vehicle::getColor))
					.mapInheritance(inheritanceConfiguration);
			assertThatThrownBy(() -> mappingBuilder.build(persistenceContext))
					.isInstanceOf(MappingConfigurationException.class)
					.hasMessage("Combination of mapped super class and inheritance is not supported, please remove one of them");
		}
		
		@Test
		void joinedTables() {
			MappedSuperClassData mappedSuperClassData = new MappedSuperClassData();
			
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					// mapped super class defines id
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.map(Vehicle::getColor)
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
					.mapInheritance(inheritanceConfiguration)
					.withJoinedTable()
					.build(persistenceContext, mappedSuperClassData.carTable);
			
			// as an inherited entity, the table should be in the context
			Collection<Table> tables = DDLDeployer.collectTables(persistenceContext);
			assertThat(Iterables.collectToList(tables, Table::getName).contains(Vehicle.class.getSimpleName())).isTrue();
			
			// DML tests
			DDLDeployer ddlDeployer = new DDLDeployer(persistenceContext);
			ddlDeployer.deployDDL();
			
			Car dummyCar = new Car(1L);
			dummyCar.setModel("Renault");
			dummyCar.setColor(new Color(666));
			
			// insert test
			carPersister.insert(dummyCar);
			
			List<Car> allCars = persistenceContext.newQuery(select(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.carTable.modelColumn, mappedSuperClassData.vehicleTable.colorColumn)
					.from(mappedSuperClassData.carTable).innerJoin(mappedSuperClassData.carTable.idColumn, mappedSuperClassData.vehicleTable.idColumn), Car.class)
					.mapKey(Car::new, mappedSuperClassData.carTable.idColumn)
					.map(mappedSuperClassData.carTable.modelColumn, Car::setModel)
					.map(mappedSuperClassData.vehicleTable.colorColumn, Car::setColor)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
			
			// select test
			Car loadedCar = carPersister.select(new PersistedIdentifier<>(1L));
			assertThat(loadedCar).isEqualTo(dummyCar);
		}
		
		@Test
		void withEmbeddable() {
			EntityMappingConfiguration<Vehicle, Identifier<Long>> inheritanceConfiguration = entityBuilder(Vehicle.class, LONG_TYPE)
					.mapKey(Vehicle::getId, ALREADY_ASSIGNED)
					.embed(Vehicle::getColor, embeddableBuilder(Color.class)
							.map(Color::getRgb))
					.getConfiguration();
			
			EntityPersister<Car, Identifier<Long>> carPersister = entityBuilder(Car.class, LONG_TYPE)
					.map(Car::getModel)
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
			assertThat(loadedCar).isEqualTo(dummyCar);
			
			// checking with query to understand what's under the hood : rgb column is created instead of color
			List<Car> allCars = persistenceContext.newQuery("select id, model, rgb from Car", Car.class)
					.mapKey((SerializableFunction<Identifier<Long>, Car>) Car::new, "id", (Class<Identifier<Long>>) (Class) Identifier.class)
					.map("model", Car::setModel)
					.map("rgb", Car::setColor, int.class, Color::new)
					.execute();
			assertThat(allCars).isEqualTo(Arrays.asList(dummyCar));
		}
	}
	
	/**
	 * Data and method for common tests of building a strategy from a mapped super class
	 */
	static class MappedSuperClassData {
		
		abstract class AbstractVehicleTable<SELF extends AbstractVehicleTable<SELF>> extends Table<SELF> {
			final Column<SELF, Identifier<Long>> idColumn = addColumn("id", (Class<Identifier<Long>>) (Class) Identifier.class).primaryKey();
			final Column<SELF, Color> colorColumn = addColumn("color", Color.class);
			
			public AbstractVehicleTable(String name) {
				super(name);
			}
		}
		
		class VehicleTable extends AbstractVehicleTable<VehicleTable> {
			
			public VehicleTable(String name) {
				super(name);
			}
		}
		
		class CarTable extends AbstractVehicleTable<CarTable> {
			final Column<CarTable, String> modelColumn = addColumn("model", String.class);
			
			public CarTable(String name) {
				super(name);
			}
		}
		
		private final VehicleTable vehicleTable = new VehicleTable("vehicle");
		
		private final CarTable carTable = new CarTable("car");
	}
	
}
